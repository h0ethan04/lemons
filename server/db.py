"""
SQLite storage for telemetry + driver stints.

Design notes (why SQLite, and why it's fast enough here):
- At most 10 clients, one writer (the ingest route), a few million rows max
  over a 7-day rolling window -> a single-file SQLite DB in WAL mode
  comfortably handles this with room to spare. WAL lets readers (history
  queries from the browser) run concurrently with the writer without
  blocking each other.
- synchronous=NORMAL (instead of the default FULL) skips an fsync on every
  commit; WAL mode still guarantees consistency, we only trade away
  durability against an OS crash in the same instant as a write, which is
  an acceptable trade for a live telemetry feed where the next sample is
  <1s away anyway.
- All DB calls in this module are plain blocking sqlite3 calls. main.py
  runs them via asyncio.to_thread() so they never block the event loop
  (and therefore never block the websocket broadcast, which is what
  actually has to hit the sub-second budget).
"""
import sqlite3
import time
from pathlib import Path

import numpy as np

from obd_fields import FIELD_NAMES

DB_PATH = Path(__file__).parent / "telemetry.db"

_FIELD_COLS_SQL = ", ".join(f'"{name}" REAL' for name in FIELD_NAMES)
_FIELD_COLS_LIST = ", ".join(f'"{name}"' for name in FIELD_NAMES)
_FIELD_PLACEHOLDERS = ", ".join(f":{name}" for name in FIELD_NAMES)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# One connection reused for the process lifetime (single-writer app, small
# client count -- no pooling needed).
_conn = get_conn()


def init_db() -> None:
    _conn.executescript(f"""
    CREATE TABLE IF NOT EXISTS stints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        driver_number INTEGER NOT NULL CHECK (driver_number BETWEEN 1 AND 6),
        driver_name TEXT NOT NULL,
        start_ts REAL NOT NULL,
        end_ts REAL
    );
    CREATE INDEX IF NOT EXISTS idx_stints_active ON stints (end_ts);

    CREATE TABLE IF NOT EXISTS telemetry (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_ts REAL NOT NULL,
        server_ts REAL NOT NULL,
        stint_id INTEGER REFERENCES stints(id),
        {_FIELD_COLS_SQL}
    );
    CREATE INDEX IF NOT EXISTS idx_telemetry_server_ts ON telemetry (server_ts);
    CREATE INDEX IF NOT EXISTS idx_telemetry_stint ON telemetry (stint_id);
    """)
    _conn.commit()


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------

def get_active_stint_id() -> int | None:
    row = _conn.execute(
        "SELECT id FROM stints WHERE end_ts IS NULL ORDER BY start_ts DESC LIMIT 1"
    ).fetchone()
    return row["id"] if row else None


def insert_telemetry(row: dict) -> int:
    """row must contain source_ts, server_ts, stint_id, and all FIELD_NAMES."""
    cur = _conn.execute(
        f"""INSERT INTO telemetry (source_ts, server_ts, stint_id, {_FIELD_COLS_LIST})
            VALUES (:source_ts, :server_ts, :stint_id, {_FIELD_PLACEHOLDERS})""",
        row,
    )
    _conn.commit()
    return cur.lastrowid


def get_recent_telemetry(seconds: float) -> list[dict]:
    """Backfill window for clients that just connected."""
    cutoff = time.time() - seconds
    rows = _conn.execute(
        f"""SELECT id, source_ts, server_ts, stint_id, {_FIELD_COLS_LIST}
            FROM telemetry WHERE server_ts >= ? ORDER BY server_ts ASC""",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


def _lttb_indices(xs: np.ndarray, ys: np.ndarray, threshold: int) -> np.ndarray:
    """Largest-Triangle-Three-Buckets downsampling. Returns indices into
    xs/ys, always including the first and last point. Picks, per bucket,
    the point that forms the largest triangle with the previously-picked
    point and the next bucket's average -- this is what keeps a transient
    spike (a downshift blip, a lockup) from being averaged away, unlike
    naive stride sampling or bucket-averaging.

    Vectorized with numpy so a single field with ~1.5M rows downsamples
    in well under a second (the loop only runs `threshold` times; the
    per-bucket area calc across ~n/threshold points is a numpy op).
    """
    n = len(xs)
    if threshold >= n or threshold <= 2:
        return np.arange(n)

    sampled = np.empty(threshold, dtype=np.int64)
    sampled[0] = 0
    sampled[-1] = n - 1
    bucket_size = (n - 2) / (threshold - 2)
    a = 0

    for i in range(threshold - 2):
        avg_start = min(int((i + 1) * bucket_size) + 1, n)
        avg_end = min(int((i + 2) * bucket_size) + 1, n)
        if avg_end > avg_start:
            avg_x = xs[avg_start:avg_end].mean()
            avg_y = ys[avg_start:avg_end].mean()
        else:
            avg_x, avg_y = xs[a], ys[a]

        range_start = min(int(i * bucket_size) + 1, n)
        range_end = min(int((i + 1) * bucket_size) + 1, n)
        if range_end <= range_start:
            range_end = range_start + 1

        ax, ay = xs[a], ys[a]
        seg_x = xs[range_start:range_end]
        seg_y = ys[range_start:range_end]
        area = np.abs((ax - avg_x) * (seg_y - ay) - (ax - seg_x) * (avg_y - ay))
        max_idx = range_start + int(np.argmax(area))
        sampled[i + 1] = max_idx
        a = max_idx

    return np.unique(sampled)


def get_history(
    start_ts: float | None = None,
    end_ts: float | None = None,
    stint_id: int | None = None,
    max_points: int = 4000,
) -> dict:
    """Bounded history query. However long the raw range is -- a few
    seconds or a 13-hour, 5Hz, ~230K-row stint -- the response never
    exceeds roughly `max_points` rows, so the client never has to hold or
    render more than that regardless of session length.

    Each of the 6 channels is downsampled independently (so every
    channel's own spikes survive), then the union of the selected
    timestamps is taken and full rows returned at those points -- this
    keeps the response shape identical to the non-downsampled case (one
    row per timestamp, every field present), so nothing downstream needs
    to know downsampling happened.
    """
    clauses, params = [], []
    if start_ts is not None:
        clauses.append("server_ts >= ?")
        params.append(start_ts)
    if end_ts is not None:
        clauses.append("server_ts <= ?")
        params.append(end_ts)
    if stint_id is not None:
        clauses.append("stint_id = ?")
        params.append(stint_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    raw_count = _conn.execute(
        f"SELECT COUNT(*) AS c FROM telemetry {where}", params
    ).fetchone()["c"]

    rows = _conn.execute(
        f"""SELECT id, source_ts, server_ts, stint_id, {_FIELD_COLS_LIST}
            FROM telemetry {where} ORDER BY server_ts ASC""",
        params,
    ).fetchall()
    rows = [dict(r) for r in rows]

    if raw_count <= max_points:
        return {"points": rows, "raw_count": raw_count, "returned_count": raw_count, "downsampled": False}

    xs = np.array([r["server_ts"] for r in rows], dtype=np.float64)
    per_field_budget = max(50, max_points // len(FIELD_NAMES))
    keep = set()
    for name in FIELD_NAMES:
        ys = np.array([r[name] if r[name] is not None else 0.0 for r in rows], dtype=np.float64)
        keep.update(_lttb_indices(xs, ys, per_field_budget).tolist())

    kept_sorted = sorted(keep)
    out_rows = [rows[i] for i in kept_sorted]
    return {
        "points": out_rows,
        "raw_count": raw_count,
        "returned_count": len(out_rows),
        "downsampled": True,
    }


def prune_older_than(days: float = 7.0) -> int:
    cutoff = time.time() - days * 86400
    cur = _conn.execute("DELETE FROM telemetry WHERE server_ts < ?", (cutoff,))
    _conn.commit()
    return cur.rowcount


# ---------------------------------------------------------------------------
# Driver stints
# ---------------------------------------------------------------------------

def start_stint(driver_number: int, driver_name: str) -> dict:
    """Only one driver is in the car at a time, so starting a stint closes
    whatever stint is currently open before opening the new one."""
    now = time.time()
    _conn.execute("UPDATE stints SET end_ts = ? WHERE end_ts IS NULL", (now,))
    cur = _conn.execute(
        "INSERT INTO stints (driver_number, driver_name, start_ts, end_ts) VALUES (?, ?, ?, NULL)",
        (driver_number, driver_name, now),
    )
    _conn.commit()
    return dict(_conn.execute("SELECT * FROM stints WHERE id = ?", (cur.lastrowid,)).fetchone())


def end_active_stint() -> dict | None:
    active = _conn.execute("SELECT * FROM stints WHERE end_ts IS NULL LIMIT 1").fetchone()
    if not active:
        return None
    now = time.time()
    _conn.execute("UPDATE stints SET end_ts = ? WHERE id = ?", (now, active["id"]))
    _conn.commit()
    return dict(_conn.execute("SELECT * FROM stints WHERE id = ?", (active["id"],)).fetchone())


def list_stints() -> list[dict]:
    rows = _conn.execute("SELECT * FROM stints ORDER BY start_ts ASC").fetchall()
    return [dict(r) for r in rows]

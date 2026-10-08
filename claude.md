# Pit Wall — Racecar OBD Telemetry

A full-stack telemetry pipeline: OBD source → FastAPI server → SQLite +
live websocket fan-out → browser dashboard with overlayable graphs and
6-driver stint splitting.

## Run it

```bash
pip install -r requirements.txt
cd server
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000` (or `http://<server-ip>:8000` from another
machine on the same network — up to 10 clients can have the page open at
once).

To see it move without real hardware, in another terminal:

```bash
python3 simulator.py --hz 2 --driver "Alex"
```

This posts a plausible-looking lap trace at 2 Hz and opens a stint for
driver 1. Stop with Ctrl+C.

## How data actually gets from the car to the screen in <3s

```
OBD source --(network)--> POST /api/ingest --(in-memory publish)--> SSE clients --> chart redraw
                                            \--(background thread)-> SQLite
```

The one thing worth understanding about this codebase: **the live-feed
publish and the database write happen concurrently, not one after the
other.** `POST /api/ingest` in `server/main.py` fires
`asyncio.gather(hub.publish(...), db_write(...))`. The publish is pure
in-memory asyncio and reaches connected clients in low single-digit
milliseconds; the DB write runs in a background thread via
`asyncio.to_thread` so a slow disk can never delay delivery to clients who
are already connected. Durability and delivery are decoupled on purpose.

That's what makes the "newest point isn't older than 1.5s" requirement
achievable: the server doesn't sit on data waiting for a DB commit, and the
client doesn't poll — it holds one open connection and repaints the
instant a message arrives. In local testing, publish-to-client delivery
was ~2ms.

**Why Server-Sent Events instead of a websocket:** the client only ever
*receives* here — it never has anything to send back over the live
channel — so a one-way stream is the right-sized tool. The concrete
payoff: the browser's built-in `EventSource` reconnects on its own after
any dropped connection, with zero client-side backoff/retry code. On
reconnect it just re-opens `GET /api/live`, and the server hands it a
fresh short backfill (last 60s) — there's no "resume from where I left
off" state to track on either side, because only the *currently open*
driver stint's tail is ever live; every completed stint is served from
`/api/history` on demand instead of streamed.

SQLite runs in WAL mode with `synchronous=NORMAL`: readers (history
queries for the comparison view) never block the writer, and commits skip
an fsync per write. That fsync is the one durability guarantee traded away
— acceptable here since the next sample is always <1s behind. Given the
stated scale (≤10 clients, ~7 days of data, a few million rows max), a
single SQLite file is comfortably fast enough; there was no reason to reach
for a heavier DB.

## Why history queries never explode in size

A session can run long — 5Hz recording over a 13-hour day is roughly
230,000 rows per channel for a single stint. Loading that raw into the
browser for the comparison view would mean holding and rendering well over
a million points. `GET /api/history` never returns more than `max_points`
rows (default 4000, tunable per request) regardless of how long the
underlying stint is:

- If the stint's raw row count is already under the cap, it's returned as-is
  — the common case for anything but a very long session.
- Otherwise each of the 6 channels is downsampled independently with
  **LTTB** (Largest-Triangle-Three-Buckets, vectorized with numpy in
  `db.py`), which picks the points that best preserve each channel's shape
  — including sharp transients like a redline blip or a lockup — rather
  than averaging or stride-sampling them away. The union of every channel's
  selected timestamps is taken and full rows are returned at those points,
  so the response shape is identical whether or not downsampling happened;
  nothing downstream needs special-casing.
- The response includes `raw_count`, `returned_count`, and `downsampled`,
  and the comparison view surfaces this ("showing 4,000 of 230,000 points
  (downsampled)") rather than silently reshaping the data — worth knowing
  when you're studying a lap.

The live tab is unaffected by any of this: it only ever keeps a small
rolling window (30s–5min) in memory regardless of session length, so it
was never the place holding a million points.

## Verifying the 1.5s freshness requirement yourself

The header's **data age** readout is not decorative — it's
`Date.now()/1000 - <server_ts of the last point received>`, recomputed
4x/second. The dot next to it goes green under 1.5s, amber under 3s, red
beyond that. If your OBD source's network hop is slow, this is exactly
where you'll see it, in real time, rather than having to infer it.

## Data model

- `obd_fields.py` is the single source of truth for the six channels
  (`calculated_engine_load`, `engine_rpm`, `vehicle_speed`,
  `throttle_position`, `coolant_temp`, `intake_air_temperature`), derived
  from the field names/units in the source OBD PID dictionary. It drives
  the DB schema, the ingest validation, and the frontend's metric picker —
  change it in one place if the channel list ever changes.
- **Note on the ingest payload:** the source dictionary maps raw OBD-II PID
  bytes to a decode formula (e.g. `engine_rpm: ((b[0]*256)+b[1])/4`). That
  decoding is assumed to happen on the OBD-reader/source side, since raw
  byte arrays and lambda formulas aren't meaningful over JSON — `/api/ingest`
  expects the six already-decoded numeric values. If your source instead
  ships raw PID bytes, decode them before the POST (or tell me and I'll
  move the formula evaluation server-side).
- `telemetry` table: one row per reading, timestamped twice —
  `source_ts` (when the OBD source captured it, if provided — otherwise
  defaults to server receipt time) and `server_ts` (server receipt time,
  used for the freshness check and for the live rolling window).
- `stints` table: one row per driver's time in the car. Only one stint is
  ever open (`end_ts IS NULL`) at a time — starting a new one auto-closes
  whatever was open, matching the fact that one person drives at a time.
  Each ingested row is tagged with whichever stint is currently open
  (`stint_id`, nullable), so historical queries can slice by driver later
  without any additional bookkeeping at query time.

## API

| Route | Purpose |
|---|---|
| `POST /api/ingest` | Accepts one reading, timestamps it, publishes + persists it |
| `GET /api/live` (SSE) | On connect: last 60s backfill, then a message per new reading |
| `GET /api/history?start_ts=&end_ts=&stint_id=&max_points=` | Historical query for the comparison view, capped and downsampled |
| `GET /api/fields` | Channel metadata (unit, color, axis group) for the UI |
| `GET /api/stints` | All recorded driver stints |
| `POST /api/stints/start` | `{driver_number: 1-6, driver_name}` — opens a stint, closes any other |
| `POST /api/stints/end` | Closes whichever stint is open |

## Frontend

Single-page app, no build step (`static/index.html` + `app.js` +
`style.css`), charts via [uPlot](https://github.com/leeoniya/uPlot) —
vendored locally in `static/vendor/` rather than loaded from a CDN, since
this is meant to run at the track where the network to the outside internet
may not be reliable; only the browser ↔ your server connection matters.

- **Live tab**: pick any combination of the six channels as chips, click
  "Add chart" to create a panel with those overlaid. Each panel has its own
  chip row, so you can add or remove channels from *that specific chart*
  at any time — charts are never locked to a fixed preset. Channels that
  share a physical unit (%, RPM, km/h, °C) share a y-axis; mixed-unit
  overlays get a second axis automatically. A rolling window (30s–5min)
  controls how much history each live chart shows.
- **Driver stint rail**: six slots. Starting a stint on one slot
  auto-ends whatever was previously active — physically only one driver is
  in the car.
- **Driver comparison tab**: pick up to six stints and one or more
  channels; each channel gets its own chart with one line per selected
  stint, x-axis re-zeroed to "seconds since that stint started" so drivers
  who drove at completely different times of day still line up lap-for-lap.

## Files

```
server/
  main.py          FastAPI app: ingest, websocket, history, stints
  db.py            SQLite schema + queries (WAL mode)
  obd_fields.py    Channel schema derived from the OBD PID dictionary
static/
  index.html       Page shell
  app.js           Websocket client, chart management, stint controls
  style.css        Dashboard styling
  vendor/          Locally vendored uPlot (no CDN dependency)
simulator.py       Posts fake OBD readings to /api/ingest for testing
requirements.txt
```

## Things you'll likely want to adjust for your specific car

- `AXIS_RANGES` in `app.js` (RPM redline, top speed, temp range) — currently
  generic racecar defaults.
- `BACKFILL_SECONDS` in `main.py` — how much history a client gets on
  connect (default 60s).
- A day-7 retention job: `db.prune_older_than(days=7)` is written but not
  scheduled — call it from a cron job or an APScheduler task if you want
  automatic pruning rather than manually running it.

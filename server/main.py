r"""
OBD racecar telemetry server.

Critical-path design for the <=3s source->server->client->render budget:

  source ---(network)---> POST /api/ingest ---(publish, in-memory)---> SSE clients ---> chart redraw
                                            \--(asyncio.to_thread)----> SQLite

The SSE publish and the DB write are fired concurrently (asyncio.gather),
and the publish itself never touches the disk, so a live client sees a
point milliseconds after the server receives it, regardless of DB write
latency. The DB write is on the critical path for durability, not for
delivery.

Server-Sent Events rather than a websocket: the client only ever needs to
*receive* live samples, never send anything back over the same channel, so
a one-way stream is the right tool. It also means the browser's built-in
EventSource reconnection handles a dropped connection for free -- no
client-side backoff/retry code, no "resume from where I left off"
bookkeeping. On reconnect the client just gets a fresh short backfill
(BACKFILL_SECONDS), which is cheap since only the *current* stint's tail
is ever "live" -- completed stints are served from /api/history on demand.
"""
import asyncio
import json
import time

from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

import db
from obd_fields import OBD_FIELDS, FIELD_NAMES

app = FastAPI(title="Racecar OBD Telemetry")

STATIC_DIR = (__import__("pathlib").Path(__file__).parent.parent / "static")

# How much history to hand a client the instant it connects (or reconnects
# after a network blip), so the live chart isn't empty while waiting for
# the next sample.
BACKFILL_SECONDS = 60

# How often to send an SSE comment as a keepalive when there's no new data,
# so idle proxies/load balancers don't silently close the connection.
KEEPALIVE_SECONDS = 15


# ---------------------------------------------------------------------------
# Fan-out hub for the live SSE stream
# ---------------------------------------------------------------------------

class LiveHub:
    """One asyncio.Queue per connected SSE client. publish() is called from
    the ingest route; each subscriber's generator drains its own queue."""

    def __init__(self):
        self._subscribers: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=500)
        async with self._lock:
            self._subscribers.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue):
        async with self._lock:
            self._subscribers.discard(q)

    async def publish(self, message: str):
        async with self._lock:
            subs = list(self._subscribers)
        for q in subs:
            try:
                q.put_nowait(message)
            except asyncio.QueueFull:
                # A stalled client shouldn't apply backpressure to ingest;
                # drop its oldest queued point and keep going.
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(message)
                except asyncio.QueueFull:
                    pass


hub = LiveHub()


history_gate = asyncio.Semaphore(1)

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class TelemetryIn(BaseModel):
    calculated_engine_load: float
    engine_rpm: float
    vehicle_speed: float
    throttle_position: float
    coolant_temp: float
    intake_air_temperature: float
    source_ts: float | None = Field(
        default=None,
        description="Epoch seconds when the OBD source captured this sample. "
                    "If omitted, the server's receipt time is used for both.",
    )


class StintStartIn(BaseModel):
    driver_number: int = Field(ge=1, le=6)
    driver_name: str


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    db.init_db()


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

@app.post("/api/ingest")
async def ingest(payload: TelemetryIn):
    server_ts = time.time()
    source_ts = payload.source_ts if payload.source_ts is not None else server_ts

    stint_id = await asyncio.to_thread(db.get_active_stint_id)

    values = {name: getattr(payload, name) for name in FIELD_NAMES}
    row = {"source_ts": source_ts, "server_ts": server_ts, "stint_id": stint_id, **values}

    message = json.dumps({"type": "live", **row})

    # Fire the SSE publish + DB write concurrently. Publish is pure
    # in-memory asyncio and returns almost immediately; the DB write runs
    # in a thread so it can never delay delivery to already-connected
    # clients.
    _, insert_id = await asyncio.gather(
        hub.publish(message),
        asyncio.to_thread(db.insert_telemetry, row),
    )

    return {"status": "ok", "id": insert_id, "server_ts": server_ts, "stint_id": stint_id}


# ---------------------------------------------------------------------------
# Live feed (Server-Sent Events)
# ---------------------------------------------------------------------------

def _sse(data: str) -> str:
    return f"data: {data}\n\n"


@app.get("/api/live")
async def live_stream(request: Request):
    q = await hub.subscribe()

    async def event_gen():
        try:
            # Tell the browser to retry fast if this connection ever drops --
            # EventSource reconnects to this same URL automatically, no
            # client code required.
            yield "retry: 1000\n\n"

            backfill = await asyncio.to_thread(db.get_recent_telemetry, BACKFILL_SECONDS)
            yield _sse(json.dumps({"type": "backfill", "points": backfill}))

            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(q.get(), timeout=KEEPALIVE_SECONDS)
                    yield _sse(message)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            await hub.unsubscribe(q)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # disable nginx response buffering, if ever fronted by one
        },
    )


# ---------------------------------------------------------------------------
# History / analysis queries
# ---------------------------------------------------------------------------

@app.get("/api/history")
async def history(
    start_ts: float | None = Query(default=None),
    end_ts: float | None = Query(default=None),
    stint_id: int | None = Query(default=None),
    max_points: int = Query(default=4000, ge=100, le=20000),
):
    """Bounded regardless of how long the underlying stint is -- see
    db.get_history's docstring for how the downsampling preserves each
    channel's spikes rather than averaging them away."""
    async with history_gate:
        return await asyncio.to_thread(db.get_history, start_ts, end_ts, stint_id, max_points)
 


@app.get("/api/fields")
async def fields():
    return {"fields": OBD_FIELDS}


# ---------------------------------------------------------------------------
# Driver stints
# ---------------------------------------------------------------------------

@app.get("/api/stints")
async def get_stints():
    return {"stints": await asyncio.to_thread(db.list_stints)}


@app.post("/api/stints/start")
async def post_start_stint(payload: StintStartIn):
    stint = await asyncio.to_thread(db.start_stint, payload.driver_number, payload.driver_name)
    return {"stint": stint}


@app.post("/api/stints/end")
async def post_end_stint():
    stint = await asyncio.to_thread(db.end_active_stint)
    if stint is None:
        raise HTTPException(status_code=400, detail="No active stint to end.")
    return {"stint": stint}


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------

@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

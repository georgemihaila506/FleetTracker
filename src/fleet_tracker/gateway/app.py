"""FastAPI gateway: one Redis subscription -> many browser WebSockets.

Lifecycle:
  * startup: open a shared Redis client, SUBSCRIBE to positions:{city}, and start
    a single background task that reads the channel and calls manager.broadcast().
  * per WebSocket: register a bounded queue, then loop pulling from that queue and
    sending to the browser (the per-connection sender).
  * shutdown: stop the subscriber task and close Redis.

The fan-out itself (broadcast) lives in manager.py — that's the ADR-0007 core.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ..consumers.zones import ZONES
from ..shared.config import get_settings
from ..shared.redis_client import make_redis
from .manager import ConnectionManager

_STATIC_DIR = Path(__file__).parent / "static"

manager = ConnectionManager()


async def _subscribe_and_fan_out(app: FastAPI) -> None:
    """The single Redis subscriber. Fans positions AND alerts out to browsers.

    Both channels ride the same WebSocket to each browser; the page tells them
    apart by shape (a Position has lat/lon, an Alert has a ``kind``). Alerts are
    events, but on the browser edge they're still best-effort (ADR-0007) — the
    durable record lives in the alerts:{city} stream, the WS toast is a courtesy.
    """
    settings = get_settings()
    redis = app.state.redis
    pubsub = redis.pubsub()
    channels = (settings.positions_channel, settings.alerts_channel)
    await pubsub.subscribe(*channels)
    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue  # skip subscribe/unsubscribe confirmations
            manager.broadcast(msg["data"])  # str, thanks to decode_responses
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(*channels)
        await pubsub.aclose()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = make_redis()
    task = asyncio.create_task(_subscribe_and_fan_out(app))
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await app.state.redis.aclose()


app = FastAPI(title="Fleet Tracker Gateway", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, object]:
    return {"status": "ok", "connections": manager.count, "dropped": manager.dropped}


@app.get("/zones")
async def zones() -> list[dict[str, object]]:
    """Geofence polygons for the map to draw (one source of truth: zones.py)."""
    return [
        {"name": z.name, "polygon": [list(pt) for pt in z.polygon]} for z in ZONES
    ]


@app.websocket("/ws")
async def ws_positions(ws: WebSocket) -> None:
    """One browser connection: snapshot -> live (ADR-0005).

    Order matters. We register the queue FIRST so live messages start buffering,
    THEN send the current-state snapshot, THEN drain live. Any vehicle that moved
    between snapshot and now is in the buffered live stream and supersedes the
    snapshot copy — because positions are level-triggered state, the race
    self-heals: the browser just sees one stale frame replaced by a fresh one.
    """
    settings = get_settings()
    queue = await manager.connect(ws)  # register -> live starts buffering
    try:
        # Cold-start snapshot: the whole fleet's latest positions in one shot,
        # including vehicles that have since gone quiet.
        snapshot = await ws.app.state.redis.hgetall(settings.positions_current_key)
        for raw in snapshot.values():
            await ws.send_text(raw)

        # Then live.
        while True:
            message = await queue.get()  # blocks this connection only
            await ws.send_text(message)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


# Any other static assets (added in M4: the Leaflet map).
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

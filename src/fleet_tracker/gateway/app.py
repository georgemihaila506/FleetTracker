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
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ..consumers.zones import ZONES
from ..replay.run import run as replay_run
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
    channels = (
        settings.positions_channel,
        settings.alerts_channel,
        settings.dropout_channel,
    )
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


@app.get("/stats")
async def stats() -> dict[str, object]:
    """Fleet rollups from the analytics read model (analytics:{city} hash).

    A plain REST read of a materialized view — the analytics consumer group keeps
    the hash current; the gateway just aggregates it on demand. The map polls this.
    """
    settings = get_settings()
    raw = await app.state.redis.hgetall(settings.analytics_key)
    vehicles = [json.loads(v) for v in raw.values()]
    if not vehicles:
        return {"vehicles": 0, "total_distance_km": 0.0, "avg_speed": 0.0, "top": []}
    total_m = sum(v["distance_m"] for v in vehicles)
    avg_speed = sum(v["avg_speed"] for v in vehicles) / len(vehicles)
    top = sorted(vehicles, key=lambda v: v["distance_m"], reverse=True)[:3]
    return {
        "vehicles": len(vehicles),
        "total_distance_km": round(total_m / 1000, 2),
        "avg_speed": round(avg_speed, 1),
        "top": [
            {"vehicle_id": v["vehicle_id"], "km": round(v["distance_m"] / 1000, 2)}
            for v in top
        ],
    }


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

        # Presence snapshot: grey any vehicles already offline, so a fresh browser
        # doesn't show a dead vehicle as live until the next transition (ADR-0005).
        offline = await ws.app.state.redis.smembers(settings.dropout_offline_key)
        for vid in offline:
            await ws.send_text(json.dumps({"vehicle_id": vid, "status": "offline"}))

        # Then live.
        while True:
            message = await queue.get()  # blocks this connection only
            await ws.send_text(message)
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)


@app.websocket("/ws/replay")
async def ws_replay(ws: WebSocket) -> None:
    """Watch history on the map (M10). Isolated from the live path on purpose.

    On connect we (a) subscribe to the replay channel and stream it to this one
    browser, and (b) kick off a replay of the last ``last`` seconds at ``speed``x.
    The effect processors never subscribe to replay:{city}, so this animation
    re-fires nothing — the point of ADR-0008.
    """
    settings = get_settings()
    await ws.accept()
    last = float(ws.query_params.get("last", 60))
    speed = float(ws.query_params.get("speed", 10))

    pubsub = ws.app.state.redis.pubsub()
    await pubsub.subscribe(settings.replay_channel)
    task = asyncio.create_task(replay_run(time.time() - last, speed))
    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            await ws.send_text(msg["data"])
    except (WebSocketDisconnect, RuntimeError):
        pass  # browser closed mid-stream
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await pubsub.unsubscribe(settings.replay_channel)
        await pubsub.aclose()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


# Any other static assets (added in M4: the Leaflet map).
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

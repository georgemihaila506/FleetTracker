"""Dropout watcher — absence detection by timeout (M9).

Every other consumer reacts to a message arriving. This one reacts to a message
*not* arriving. A dead or disconnected vehicle publishes nothing — and you can't
subscribe to silence — so this watcher manufactures the "offline" event itself:
it remembers when it last heard from each vehicle (every position doubles as a
heartbeat, ADR-0002) and, on its own timer, flags anyone who's gone quiet for too
long. When they publish again, it flags them back online.

Two things run concurrently:
  * ``_track``  — subscribes to positions and stamps ``last_seen[vehicle_id]``.
  * a scan loop — every ``dropout_scan_interval_s``, diffs silence against the
    threshold and emits offline/online transitions.

State:
  * ``dropout:offline:{city}`` (Redis set) — who is currently offline, so a fresh
    browser can grey the right dots on connect.
  * ``dropouts:{city}`` (stream) + ``dropout:{city}`` (channel) — the durable event
    log and the live push, same event/state split as every other event here.

Run:   python -m fleet_tracker.consumers.dropout
Check: docker compose exec redis redis-cli SMEMBERS dropout:offline:testcity
"""

from __future__ import annotations

import asyncio
import time

from ..shared.config import Settings, get_settings
from ..shared.models import Position, Presence
from ..shared.redis_client import redis_client


def _detect_transitions(
    last_seen: dict[str, float], offline: set[str], now: float, threshold: float
) -> tuple[set[str], set[str]]:
    """Turn heartbeat times into offline/online *transitions*.

    Given each vehicle's last-heard time (``last_seen``), who is currently flagged
    ``offline``, the current time ``now``, and the silence ``threshold``, returns
    ``(newly_offline, newly_online)``:
      * ``newly_offline`` — heard from before, silent longer than ``threshold``,
        and not already flagged. The ``not in offline`` guard is what makes this
        edge-triggered: a long-dead vehicle is reported once, not every scan.
      * ``newly_online``  — currently flagged offline but heard from within
        ``threshold`` (recovered).

    The time-based twin of geofence's set-difference edge detection: identical
    "emit only on change" shape, but the trigger is a clock, not a stream entry.
    A vehicle never heard from (absent from ``last_seen``) is reported by neither.
    """
    newly_offline = {
        v for v, ts in last_seen.items() if now - ts > threshold and v not in offline
    }
    newly_online = {
        v for v in offline if v in last_seen and now - last_seen[v] <= threshold
    }
    return newly_offline, newly_online


async def _track(r, settings: Settings, last_seen: dict[str, float]) -> None:
    """Heartbeat listener: every position refreshes that vehicle's last_seen."""
    pubsub = r.pubsub()
    await pubsub.subscribe(settings.positions_channel)
    try:
        async for msg in pubsub.listen():
            if msg["type"] != "message":
                continue
            pos = Position.from_wire(msg["data"])
            last_seen[pos.vehicle_id] = time.time()  # arrival time, not pos.ts
    except asyncio.CancelledError:
        pass
    finally:
        await pubsub.unsubscribe(settings.positions_channel)
        await pubsub.aclose()


async def _emit(r, settings: Settings, vehicle_id: str, status: str) -> None:
    """Record a presence transition: durable XADD + live PUBLISH."""
    ev = Presence(vehicle_id=vehicle_id, status=status)
    wire = ev.to_wire()
    await r.xadd(
        settings.dropout_stream,
        {"data": wire},
        maxlen=settings.stream_maxlen,
        approximate=True,
    )
    await r.publish(settings.dropout_channel, wire)
    print(f"  {status.upper():7} {vehicle_id}")


async def _scan_once(r, settings: Settings, last_seen: dict[str, float]) -> None:
    """One sweep: detect transitions, update the offline set, emit events."""
    now = time.time()
    offline = set(await r.smembers(settings.dropout_offline_key))
    newly_offline, newly_online = _detect_transitions(
        last_seen, offline, now, settings.dropout_threshold_s
    )
    for vid in newly_offline:
        await r.sadd(settings.dropout_offline_key, vid)
        await _emit(r, settings, vid, "offline")
    for vid in newly_online:
        await r.srem(settings.dropout_offline_key, vid)
        await _emit(r, settings, vid, "online")


async def run() -> None:
    settings = get_settings()
    last_seen: dict[str, float] = {}
    async with redis_client(settings) as r:
        print(
            f"dropout: watching {settings.positions_channel}, "
            f"threshold {settings.dropout_threshold_s}s  (Ctrl-C to stop)"
        )
        track = asyncio.create_task(_track(r, settings, last_seen))
        try:
            while True:
                await asyncio.sleep(settings.dropout_scan_interval_s)
                await _scan_once(r, settings, last_seen)
        except asyncio.CancelledError:
            pass
        finally:
            track.cancel()
            await asyncio.gather(track, return_exceptions=True)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\ndropout: stopped")


if __name__ == "__main__":
    main()

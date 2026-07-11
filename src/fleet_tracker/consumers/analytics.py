"""Analytics consumer — a SECOND consumer group on the same stream (M8).

Where geofence answers "did something happen?", analytics answers "how much?":
per-vehicle cumulative distance and a rolling average speed, kept in the Redis
hash ``analytics:{city}``. It reads the exact same ``telemetry:{city}`` stream as
geofence, but through its own group with its own cursor — so both groups see
every entry independently. That's **fan-out via consumer groups**: adding a group
multiplies the data, it doesn't divide it (ADR-0003).

The deliberate contrast with geofence is idempotency. Geofence edge-detects, so a
replayed entry changes nothing. Analytics **accumulates** — a replayed entry adds
its distance again → the totals drift up. We accept that small drift here
(ADR-0004); the mitigation (store a per-vehicle last-processed id and skip
anything ``<=`` it) is a noted future step. This is exactly why "make your
consumers idempotent" is easy advice and hard practice: some computations just
aren't.

The consumer-group loop is shared with geofence (``stream_group.py``); this module
supplies the group name + the ``_process`` handler.

Run:   python -m fleet_tracker.consumers.analytics
Check: docker compose exec redis redis-cli HGETALL analytics:testcity
"""

from __future__ import annotations

import asyncio
import json

from ..shared.config import Settings, get_settings
from ..shared.geo import haversine_m
from ..shared.models import Position
from .stream_group import run_group


def _fresh_stats(vehicle_id: str) -> dict:
    """The zero rollup for a vehicle we've never seen."""
    return {
        "vehicle_id": vehicle_id,
        "distance_m": 0.0,  # cumulative metres travelled
        "samples": 0,  # positions folded in so far
        "avg_speed": 0.0,  # rolling mean of speed (m/s)
        "last_lat": None,  # previous point, for the next distance gap
        "last_lon": None,
        "last_seen": 0.0,  # ts of the most recent position
    }


async def _load_stats(r, settings: Settings, vehicle_id: str) -> dict:
    """Load a vehicle's rollup from Redis (a fresh zero one if unseen)."""
    raw = await r.hget(settings.analytics_key, vehicle_id)
    return json.loads(raw) if raw else _fresh_stats(vehicle_id)


async def _store_stats(r, settings: Settings, vehicle_id: str, stats: dict) -> None:
    """Persist a vehicle's updated rollup."""
    await r.hset(settings.analytics_key, vehicle_id, json.dumps(stats))


async def _process(r, settings: Settings, source_id: str, pos: Position) -> None:
    """Fold one position into this vehicle's running stats.

    Loads the rollup so far, then accumulates:
      * **distance** — adds the great-circle gap from the previous point to this
        one (skipped for a vehicle's very first position, which has no previous).
      * **avg speed** — a rolling mean maintained incrementally
        (``avg += (x - avg) / n``), so we never store the running sum.
      * **last point / last_seen** — remembered for the next gap and freshness.

    There is no diff here — every call *adds*, which is exactly what makes this
    consumer non-idempotent: reprocess the same entry (e.g. an XAUTOCLAIM
    redelivery after a crash) and ``samples`` over-counts and distance can grow
    twice. We accept that drift (ADR-0004); contrast geofence's `_process`, whose
    set-difference logic makes a replay a harmless no-op. The accumulator is the
    price of the answer.
    """
    stats = await _load_stats(r, settings, pos.vehicle_id)
    if stats["last_lat"] is not None:
        stats["distance_m"] += haversine_m((stats["last_lat"], stats["last_lon"]), (pos.lat, pos.lon))

    stats["samples"] += 1
    if pos.speed is not None:
        stats["avg_speed"] += (pos.speed - stats["avg_speed"]) / stats["samples"]
    
    stats["last_lat"] = pos.lat
    stats["last_lon"] = pos.lon
    stats["last_seen"] = pos.ts
    await _store_stats(r, settings, pos.vehicle_id, stats)


async def run() -> None:
    settings = get_settings()
    await run_group(
        settings,
        stream=settings.telemetry_stream,
        group=settings.analytics_group,
        handler=_process,
        label="analytics",
    )


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nanalytics: stopped")


if __name__ == "__main__":
    main()

"""Geofence consumer — the idempotent, crash-recoverable event processor (M7).

This is the payoff of the whole two-path design. It reads the DURABLE telemetry
stream through a **consumer group** (not Pub/Sub), so:
  * it can fall behind or crash without losing entries (they wait in the stream);
  * an un-acked entry from a dead consumer is reclaimed and reprocessed
    (XAUTOCLAIM) — at-least-once delivery;
  * therefore processing MUST be idempotent, or a redelivery double-fires alerts.

The idempotency lives in `_process` (YOUR core): we only emit an alert on a
*transition* (a vehicle that wasn't in a zone now is, or vice-versa), computed
against durable ``was_inside`` state in Redis. Reprocessing the same position
recomputes the same membership → no transition → no duplicate alert. That's
**effectively-once = at-least-once delivery + idempotent processing** (ADR-0004).

The consumer-group loop itself lives in ``stream_group.py`` (shared with the M8
analytics consumer); this module supplies the group name + the ``_process``
handler.

Run:   python -m fleet_tracker.consumers.geofence
Watch: docker compose exec redis redis-cli XRANGE alerts:testcity - +
"""

from __future__ import annotations

import asyncio
import json

from ..shared.config import Settings, get_settings
from ..shared.models import Alert, Position
from .stream_group import run_group
from .zones import zones_containing


async def _load_inside(r, settings: Settings, vehicle_id: str) -> set[str]:
    """The durable ``was_inside`` set for one vehicle (empty if unseen)."""
    raw = await r.hget(settings.geofence_inside_key, vehicle_id)
    return set(json.loads(raw)) if raw else set()


async def _store_inside(r, settings: Settings, vehicle_id: str, zones: set[str]) -> None:
    """Persist the new membership so it survives a consumer crash."""
    await r.hset(settings.geofence_inside_key, vehicle_id, json.dumps(sorted(zones)))


async def _emit(r, settings: Settings, pos: Position, zone: str, kind: str, source_id: str) -> None:
    """Record a crossing: durable XADD to the alert stream + live PUBLISH toast."""
    alert = Alert(
        vehicle_id=pos.vehicle_id, zone=zone, kind=kind, ts=pos.ts, source_id=source_id
    )
    wire = alert.to_wire()
    await r.xadd(
        settings.alerts_stream, {"data": wire},
        maxlen=settings.stream_maxlen, approximate=True,
    )
    await r.publish(settings.alerts_channel, wire)
    print(f"  ALERT {kind.upper():5} {pos.vehicle_id} {zone}  (src {source_id})")


async def _process(r, settings: Settings, source_id: str, pos: Position) -> None:
    """Edge-detect zone crossings for one position and emit ENTER/EXIT alerts.

    Why this is idempotent: if this exact entry is redelivered (consumer crashed
    before XACK, XAUTOCLAIM hands it back), ``now_inside`` recomputes identical to
    the ``was_inside`` we already stored → both diffs are empty → nothing re-fires.
    The durable state is what makes the replay a no-op.
    """
    now_inside = zones_containing(pos.lat, pos.lon)
    was_inside = await _load_inside(r, settings, pos.vehicle_id)
    entered = now_inside - was_inside
    exited = was_inside - now_inside
    for zone in entered:
        await _emit(r, settings, pos, zone, "enter", source_id=source_id)
    for zone in exited:
        await _emit(r, settings, pos, zone, "exit", source_id=source_id)
    await _store_inside(r, settings, pos.vehicle_id, now_inside)


async def run() -> None:
    settings = get_settings()
    await run_group(
        settings,
        stream=settings.telemetry_stream,
        group=settings.geofence_group,
        handler=_process,
        label="geofence",
    )


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\ngeofence: stopped")


if __name__ == "__main__":
    main()

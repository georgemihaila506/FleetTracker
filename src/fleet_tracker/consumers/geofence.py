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

Run:   python -m fleet_tracker.consumers.geofence
Watch: docker compose exec redis redis-cli XRANGE alerts:testcity - +
"""

from __future__ import annotations

import asyncio
import json
import os

import redis.exceptions

from ..shared.config import Settings, get_settings
from ..shared.models import Alert, Position
from ..shared.redis_client import redis_client
from .zones import zones_containing

# How long an entry must sit un-acked in a dead consumer's pending list before a
# live consumer may steal it (XAUTOCLAIM). The crash-recovery threshold: too low
# and a briefly-slow consumer gets its work stolen; too high and recovery lags.
_MIN_IDLE_MS = 5_000


async def _ensure_group(r, settings: Settings) -> None:
    """Create the consumer group at the stream tail (idempotent)."""
    try:
        # id="$" => the group starts reading only entries added from now on, so a
        # fresh run doesn't replay the whole retained backlog. mkstream creates
        # the stream if the simulator hasn't run yet.
        await r.xgroup_create(
            settings.telemetry_stream, settings.geofence_group, id="$", mkstream=True
        )
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):  # group already exists -> fine
            raise


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


async def _reclaim(r, settings: Settings, consumer: str) -> None:
    """Steal and reprocess entries abandoned by a crashed consumer (XAUTOCLAIM).

    Safe precisely because _process is idempotent: reprocessing a reclaimed entry
    can't double-fire an alert. This is the mechanism behind 'no lost events.'
    """
    cursor = "0-0"
    while True:
        result = await r.xautoclaim(
            settings.telemetry_stream, settings.geofence_group, consumer,
            min_idle_time=_MIN_IDLE_MS, start_id=cursor, count=50,
        )
        cursor, messages = result[0], result[1]
        for entry_id, fields in messages:
            if fields and "data" in fields:
                await _process(r, settings, entry_id, Position.from_wire(fields["data"]))
            await r.xack(settings.telemetry_stream, settings.geofence_group, entry_id)
        if cursor == "0-0":  # walked the whole pending list
            break


async def run() -> None:
    settings = get_settings()
    consumer = f"geofence-{os.getpid()}"  # unique per process (crash-recovery)
    async with redis_client(settings) as r:
        await _ensure_group(r, settings)
        print(
            f"geofence: group '{settings.geofence_group}' consumer '{consumer}' "
            f"on {settings.telemetry_stream}  (Ctrl-C to stop)"
        )
        try:
            while True:
                # 1. Recover anything a dead consumer left un-acked, then...
                await _reclaim(r, settings, consumer)
                # 2. ...read new entries for this consumer ('>' = never-delivered).
                resp = await r.xreadgroup(
                    settings.geofence_group, consumer,
                    {settings.telemetry_stream: ">"}, count=100, block=2000,
                )
                if not resp:
                    continue  # block timed out; loop to reclaim + read again
                for _stream, entries in resp:
                    for entry_id, fields in entries:
                        await _process(
                            r, settings, entry_id, Position.from_wire(fields["data"])
                        )
                        # XACK only AFTER processing: a crash before this leaves the
                        # entry pending -> it gets reclaimed, not lost.
                        await r.xack(
                            settings.telemetry_stream, settings.geofence_group, entry_id
                        )
        except asyncio.CancelledError:
            pass


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\ngeofence: stopped")


if __name__ == "__main__":
    main()

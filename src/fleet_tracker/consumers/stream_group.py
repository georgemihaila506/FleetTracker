"""Reusable Redis Streams consumer-group runner.

Geofence (M7) and analytics (M8) are structurally identical: read a durable
stream through a consumer group, process each entry, ``XACK`` it, and reclaim
entries a crashed consumer left un-acked (``XAUTOCLAIM``). They differ ONLY in
what "process" means. That shared loop lives here; each consumer supplies a
``handler``.

This is what "a second consumer group" means in practice: analytics is just this
same runner with a different ``group`` name and a different ``handler``, reading
the same ``telemetry:{city}`` through its own independent cursor. Two groups on
one stream = fan-out (each sees every entry); that's the M8 headline.

A handler is:  ``async def handler(r, settings, source_id, pos) -> None``
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable

import redis.exceptions

from ..shared.config import Settings
from ..shared.models import Position
from ..shared.redis_client import redis_client

Handler = Callable[["object", Settings, str, Position], Awaitable[None]]

# How long an entry must sit un-acked in a dead consumer's pending list before a
# live consumer may steal it. Too low: a briefly-slow consumer loses its work;
# too high: crash recovery lags.
_MIN_IDLE_MS = 5_000


async def _ensure_group(r, stream: str, group: str) -> None:
    """Create the group at the stream tail (id='$'), idempotently."""
    try:
        await r.xgroup_create(stream, group, id="$", mkstream=True)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):  # already exists -> fine
            raise


async def _reclaim(r, settings: Settings, stream: str, group: str, consumer: str, handler: Handler) -> None:
    """Steal + reprocess entries abandoned by a crashed consumer (XAUTOCLAIM).

    Whether reprocessing is harmless depends on the handler: an idempotent one
    (geofence) shrugs it off; an accumulating one (analytics) double-counts — the
    deliberate trade-off of ADR-0004.
    """
    cursor = "0-0"
    while True:
        result = await r.xautoclaim(
            stream, group, consumer, min_idle_time=_MIN_IDLE_MS, start_id=cursor, count=50
        )
        cursor, messages = result[0], result[1]
        for entry_id, fields in messages:
            if fields and "data" in fields:
                await handler(r, settings, entry_id, Position.from_wire(fields["data"]))
            await r.xack(stream, group, entry_id)
        if cursor == "0-0":  # walked the whole pending list
            break


async def run_group(settings: Settings, *, stream: str, group: str, handler: Handler, label: str) -> None:
    """Run the consumer-group loop for ``group`` on ``stream``, dispatching to ``handler``."""
    consumer = f"{group}-{os.getpid()}"  # unique per process (crash recovery)
    async with redis_client(settings) as r:
        await _ensure_group(r, stream, group)
        print(f"{label}: group '{group}' consumer '{consumer}' on {stream}  (Ctrl-C to stop)")
        try:
            while True:
                # 1. Recover anything a dead consumer left un-acked, then...
                await _reclaim(r, settings, stream, group, consumer, handler)
                # 2. ...read new entries for this consumer ('>' = never-delivered).
                resp = await r.xreadgroup(group, consumer, {stream: ">"}, count=100, block=2000)
                if not resp:
                    continue  # block timed out; loop to reclaim + read again
                for _stream, entries in resp:
                    for entry_id, fields in entries:
                        if fields and "data" in fields:
                            await handler(r, settings, entry_id, Position.from_wire(fields["data"]))
                        # XACK only AFTER processing: a crash before this leaves
                        # the entry pending -> it gets reclaimed, not lost.
                        await r.xack(stream, group, entry_id)
        except asyncio.CancelledError:
            pass

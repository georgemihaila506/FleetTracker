"""Replay tool — read durable telemetry history and re-emit it, isolated (M10).

This is the capability the whole durable-log half exists to enable: the stream
*remembers*, so ``XRANGE`` can pull a past window back out and we can re-animate
it. Pub/Sub could never do this — it stores nothing.

The catch, and the lesson (ADR-0008): we re-emit to a dedicated ``replay:{city}``
channel that ONLY the UI reads. The geofence/analytics/dropout consumers subscribe
to ``telemetry``/``positions``, never to ``replay:``, so replaying yesterday's data
does NOT re-fire yesterday's alerts or re-run analytics. Note this is *not* solved
by idempotency: idempotency stops duplicates within one timeline, but replay
injects a whole old timeline that looks new. Isolation — a channel the effect
processors don't listen on — is the only fix.

Usage (from the repo root, with the venv interpreter):
    python -m fleet_tracker.replay --last 60 --speed 10     # last 60s, 10x faster
    python -m fleet_tracker.replay --from 1783792000 --speed 5
    python -m fleet_tracker.replay --speed 20               # whole retained stream

Watch it:  docker compose exec redis redis-cli SUBSCRIBE replay:testcity
"""

from __future__ import annotations

import argparse
import asyncio
import time

from ..shared.config import Settings, get_settings
from ..shared.redis_client import redis_client


async def _replay(r, settings: Settings, events: list[tuple[float, str]], speed: float) -> None:
    """YOUR CORE — re-emit history to the replay channel, paced by original time.

    ``events`` is a chronological list of ``(ts_seconds, wire)`` — each ``wire`` is
    a Position JSON string exactly as it was first published, ``ts_seconds`` is
    when that entry landed in the log.

    Re-emit them so they play back at the ORIGINAL tempo divided by ``speed``:
      * walk ``events`` in order, tracking the previous event's ``ts``.
      * before emitting each one (after the first), sleep the original gap scaled
        by speed: ``(ts - prev_ts) / speed`` seconds. So a real 2s gap at
        ``speed=10`` becomes a 0.2s pause — history, 10x faster. (Guard against a
        negative/zero sleep.)
      * publish each ``wire`` to ``settings.replay_channel`` (NOT the live
        positions channel — that isolation is the whole point).

    ``asyncio.sleep`` and ``r.publish`` are all you need.
    """
    prev_ts = None
    for ts, wire in events:
        if prev_ts is not None:
            delay = (ts - prev_ts) / speed
            if delay > 0:
                await asyncio.sleep(delay)
        await r.publish(settings.replay_channel, wire)
        prev_ts = ts

async def run(from_ts: float | None, speed: float) -> None:
    settings = get_settings()
    start = f"{int(from_ts * 1000)}-0" if from_ts else "-"
    async with redis_client(settings) as r:
        entries = await r.xrange(settings.telemetry_stream, min=start, max="+")
        # Turn each stream entry into (ts_seconds, wire); the entry id's <ms> part
        # is the log's own timestamp, monotonic and already in XRANGE order.
        events = [(int(eid.split("-")[0]) / 1000.0, f["data"]) for eid, f in entries]
        if not events:
            print("nothing to replay in that window "
                  "(is the telemetry stream populated? run the simulator)")
            return
        span = events[-1][0] - events[0][0]
        print(
            f"replaying {len(events)} entries ({span:.0f}s of history) to "
            f"{settings.replay_channel} at {speed}x  (~{span / speed:.0f}s wall-clock)"
        )
        await _replay(r, settings, events, speed)
        print("replay complete")


def main() -> None:
    ap = argparse.ArgumentParser(prog="fleet_tracker.replay", description=__doc__)
    ap.add_argument("--from", dest="from_ts", type=float, default=None,
                    help="start epoch seconds (default: whole retained stream)")
    ap.add_argument("--last", type=float, default=None,
                    help="replay the last N seconds of history")
    ap.add_argument("--speed", type=float, default=10.0,
                    help="playback speed multiplier (default: 10x)")
    args = ap.parse_args()

    from_ts = args.from_ts
    if args.last is not None:
        from_ts = time.time() - args.last

    try:
        asyncio.run(run(from_ts, args.speed))
    except KeyboardInterrupt:
        print("\nreplay: stopped")


if __name__ == "__main__":
    main()

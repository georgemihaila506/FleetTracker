"""Inspect the durable telemetry stream (M6) — read-only.

Run this while the simulator is going to *see* what the Streams path gives you
that Pub/Sub never could: a persistent, ordered, replayable log.

    .venv/Scripts/python.exe scripts/inspect_stream.py

It prints:
  * XLEN vs the configured MAXLEN — watch it climb, then plateau at the cap once
    trimming kicks in (the log stops growing in RAM).
  * the oldest and newest entries with their monotonic <ms>-<seq> ids.
  * the retention window (MAXLEN / production rate, ADR-0006) — how far back the
    stream reaches, i.e. the longest a consumer can be offline without losing data.

The equivalent raw commands (worth trying in redis-cli):
    XLEN telemetry:testcity
    XRANGE telemetry:testcity - + COUNT 2
    XREVRANGE telemetry:testcity + - COUNT 2
"""

from __future__ import annotations

import asyncio
import json

from fleet_tracker.shared.config import get_settings
from fleet_tracker.shared.redis_client import redis_client


def _describe(label: str, entries: list) -> None:
    for entry_id, fields in entries:
        pos = json.loads(fields["data"])
        print(
            f"  {label:>6}: id={entry_id}  "
            f"{pos['vehicle_id']} @ ({pos['lat']:.5f}, {pos['lon']:.5f})"
        )


async def main() -> None:
    s = get_settings()
    async with redis_client() as r:
        stream = s.telemetry_stream
        n = await r.xlen(stream)

        rate = s.vehicle_count * s.tick_hz
        print(f"stream:            {stream}")
        print(f"XLEN:              {n}   (trim target: MAXLEN ~ {s.stream_maxlen})")
        print(f"production rate:   {s.vehicle_count} vehicles x {s.tick_hz} Hz "
              f"= {rate:.0f} entries/sec")
        print(f"retention window:  MAXLEN / rate = {s.retention_seconds:.0f} s "
              f"({s.retention_seconds / 60:.1f} min)")

        if n == 0:
            print("\n(stream empty — start the simulator: "
                  "python -m fleet_tracker.simulator)")
            return

        oldest = await r.xrange(stream, count=1)
        newest = await r.xrevrange(stream, count=1)
        print()
        _describe("oldest", oldest)
        _describe("newest", newest)

        # Time actually spanned by what's currently retained (from the ids).
        old_ms = int(oldest[0][0].split("-")[0])
        new_ms = int(newest[0][0].split("-")[0])
        span_s = (new_ms - old_ms) / 1000
        capped = "CAPPED — oldest entries are being trimmed" if n >= s.stream_maxlen \
            else "still growing (below the cap)"
        print(f"\nretained span:     {span_s:.1f} s across {n} entries  [{capped}]")


if __name__ == "__main__":
    asyncio.run(main())

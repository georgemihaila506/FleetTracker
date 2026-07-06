"""Materializer — keeps positions:current:{city} in sync with the live stream.

This is the CQRS "read model" of ADR-0005. Pub/Sub can't answer "what's the whole
fleet doing right now?" for a browser that just connected — a subscriber only
hears messages sent *after* it subscribed. So one dedicated consumer listens to
positions:{city} and, for each Position, records it in a Redis HASH keyed by
vehicle_id. That hash is always the current snapshot: HGETALL returns every
vehicle's latest position in one shot, including vehicles that have since gone
quiet (their last-known position stays until overwritten or trimmed).

Why a separate process and not "just have the gateway do it"?
  Separation of concerns (ADR-0003). The gateway's job is fan-out; the read model
  is a different responsibility with its own failure/scaling story. One writer
  owning the hash also means no two components race to maintain it.

Run:  python -m fleet_tracker.consumers.materializer
Check: docker compose exec redis redis-cli HGETALL positions:current:testcity
"""

from __future__ import annotations

import asyncio

from ..shared.config import get_settings
from ..shared.models import Position
from ..shared.redis_client import redis_client


async def run() -> None:
    settings = get_settings()
    print(
        f"materializer: {settings.positions_channel} -> "
        f"HASH {settings.positions_current_key}  (Ctrl-C to stop)"
    )

    async with redis_client(settings) as r:
        pubsub = r.pubsub()
        await pubsub.subscribe(settings.positions_channel)
        try:
            async for msg in pubsub.listen():
                if msg["type"] != "message":
                    continue
                raw = msg["data"]  # the JSON string the simulator published
                pos = Position.from_wire(raw)  # parse -> gives us vehicle_id, ts
                await r.hset(settings.positions_current_key, pos.vehicle_id, raw)
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe(settings.positions_channel)
            await pubsub.aclose()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nmaterializer: stopped")


if __name__ == "__main__":
    main()

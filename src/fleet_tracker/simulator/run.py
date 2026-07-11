"""The simulator loop — YOUR part to fill in.

Everything around the publish step is done: config is loaded, one shared Redis
client is opened, the fleet is built around the city centre, the loop ticks at
the configured rate, and Ctrl-C shuts down cleanly. The one thing left is the
heart of M2 — turning each vehicle into a Position and PUBLISHing it.

Run:    python -m fleet_tracker.simulator
Watch:  docker compose exec redis redis-cli SUBSCRIBE positions:testcity
"""

from __future__ import annotations

import asyncio

from ..shared.config import get_settings
from ..shared.models import Position
from ..shared.redis_client import redis_client
from .route_vehicle import make_route_fleet


async def run() -> None:
    settings = get_settings()

    # M5: vehicles follow real Bucharest streets (cached OSRM routes) instead of
    # wandering. The city name still only drives the Redis channel, not the map.
    fleet = make_route_fleet(settings.vehicle_count)

    print(
        f"simulator: {len(fleet)} vehicles -> {settings.positions_channel} "
        f"@ {settings.tick_hz} Hz  (Ctrl-C to stop)"
    )

    async with redis_client(settings) as r:
        while True:
            # Advance every vehicle by one tick of motion.
            for v in fleet:
                v.step(settings.tick_interval)

            delivered = 0
            for v in fleet:
                pos = Position(
                    vehicle_id=v.vehicle_id,
                    lat=v.lat,
                    lon=v.lon,
                    speed=v.speed,
                    heading=v.heading,
                )
                wire = pos.to_wire()

                # Ephemeral state path (M2): fire-and-forget to live subscribers.
                # Redis stores nothing; a slow/absent subscriber just misses it.
                delivered += await r.publish(settings.positions_channel, wire)

                # Durable event path (M6): append the SAME bytes to the telemetry
                # stream so they survive for consumers that read later (geofence,
                # analytics) — at-least-once instead of at-most-once.
                await r.xadd(
                    settings.telemetry_stream,
                    {"data": wire},
                    maxlen=settings.stream_maxlen,
                    approximate=True,  # MAXLEN ~ : cheap, trims whole nodes
                )

            print(f"tick: published {len(fleet)}, delivered {delivered}")
            await asyncio.sleep(settings.tick_interval)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nsimulator: stopped")


if __name__ == "__main__":
    main()

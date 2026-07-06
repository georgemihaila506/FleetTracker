"""Vehicle simulator — the producer.

Spawns N fake vehicles that random-walk around a city and, every tick, PUBLISH
their current Position (state) to positions:{city}. This is the state -> Pub/Sub
half of the spine (ADR-0002).

Run it:  python -m fleet_tracker.simulator
Watch:   docker compose exec redis redis-cli SUBSCRIBE positions:testcity
"""

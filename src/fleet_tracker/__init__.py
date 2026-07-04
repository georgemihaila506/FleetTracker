"""Live Fleet Tracker — a hands-on Redis Pub/Sub + Streams learning project.

Subpackages are added per milestone (see PLAN.md):
    shared/       message models (pydantic), redis helpers, config
    simulator/    asyncio vehicle producer
    gateway/      FastAPI WebSocket fan-out + REST + static UI
    consumers/    geofence, analytics, materializer, dropout watcher
    replay/       XRANGE-based replay CLI (isolated channel)

Design rationale lives in docs/adr/ (0001-0008); vocabulary in docs/glossary.md.
"""

__version__ = "0.1.0"

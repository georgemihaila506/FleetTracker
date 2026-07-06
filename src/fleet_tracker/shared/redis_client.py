"""Async Redis connection factory.

Every component (simulator, gateway, consumers) talks to Redis through here, so
connection setup lives in exactly one place. Built on redis-py's asyncio client.

Two things worth knowing:

* **``decode_responses=True``** — replies come back as ``str``, not ``bytes``. We
  put JSON strings on the wire (positions, later stream fields), so decoding at
  the client keeps the rest of the code free of ``.decode()`` noise.

* **Connection pooling is automatic.** ``Redis.from_url`` builds a pool; a single
  client is safe to share across an asyncio app, and each ``await`` borrows a
  connection from the pool. So we create *one* client per process and reuse it —
  don't make a new one per publish.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

import redis.asyncio as redis

from .config import Settings, get_settings


def make_redis(settings: Settings | None = None) -> redis.Redis:
    """Create a shared async Redis client from settings.

    Caller owns the lifecycle: use it for the life of the process and
    ``await client.aclose()`` on shutdown (or use ``redis_client()`` below).
    """
    settings = settings or get_settings()
    return redis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )


@asynccontextmanager
async def redis_client(
    settings: Settings | None = None,
) -> AsyncIterator[redis.Redis]:
    """Async context manager that opens a client and closes it on exit.

        async with redis_client() as r:
            await r.publish(channel, payload)
    """
    client = make_redis(settings)
    try:
        yield client
    finally:
        await client.aclose()

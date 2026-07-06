"""ConnectionManager — tracks browser WebSockets and fans messages out to them.

This is the beating heart of ADR-0007 (edge fan-out). The shape:

  * Each connected browser gets its OWN bounded asyncio.Queue.
  * The Redis subscriber calls broadcast(msg) ONCE per position message; it drops
    a copy into every connection's queue.
  * Each connection has its own sender task (in app.py) that pulls from its queue
    and awaits ws.send_text(). A slow browser drains slowly.

Why per-connection queues instead of "just await ws.send() in a loop"?
  If the subscriber awaited send() directly, one slow/stuck browser would block
  the loop and stall delivery to everyone else — head-of-line blocking. Giving
  each connection its own bounded buffer decouples them: the subscriber never
  waits on any single client.

Why BOUNDED (maxsize) and why DROP when full?
  Positions are STATE (ADR-0002). If a browser can't keep up, the right thing is
  to drop stale positions, not to buffer unboundedly (memory blowup) or block the
  whole fan-out. A dropped position is harmless — the next tick carries a fresher
  one. That is the load-shedding you (the author of broadcast) get to implement.
"""

from __future__ import annotations

import asyncio

from fastapi import WebSocket

# Max positions buffered per browser before we start shedding. At ~1 msg/vehicle
# /tick this is a fraction of a second of slack — enough to absorb a hiccup,
# small enough that a wedged client is cut loose fast.
QUEUE_MAXSIZE = 256


class ConnectionManager:
    def __init__(self) -> None:
        # One bounded queue per connected browser.
        self._queues: dict[WebSocket, asyncio.Queue[str]] = {}
        self.dropped = 0  # count of shed messages, for observability

    async def connect(self, ws: WebSocket) -> asyncio.Queue[str]:
        """Accept the socket and register a fresh bounded queue for it."""
        await ws.accept()
        queue: asyncio.Queue[str] = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
        self._queues[ws] = queue
        return queue

    def disconnect(self, ws: WebSocket) -> None:
        """Forget a socket (on close / error). Idempotent."""
        self._queues.pop(ws, None)

    @property
    def count(self) -> int:
        return len(self._queues)

    def broadcast(self, message: str) -> None:
        """Fan `message` out to every connected browser's queue."""
        for queue in list(self._queues.values()):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                self.dropped += 1

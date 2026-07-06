"""Gateway — the edge fan-out (ADR-0007).

A FastAPI service with ONE Redis Pub/Sub subscription that fans every position
message out to every connected browser over WebSockets. "Subscribers multiply":
the gateway subscribes once; each browser is just another cheap send.

Per-connection bounded queues keep a slow browser from stalling the others
(no head-of-line blocking) — a full queue drops, it never blocks the subscriber.

Run:  uvicorn fleet_tracker.gateway.app:app --reload
"""

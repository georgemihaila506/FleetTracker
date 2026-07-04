# ADR 0002 — Two messaging paths, split by state vs. event

**Status:** Accepted
**Date:** 2026-07-04

## Context
The project's core learning goal is to feel the difference between ephemeral
fan-out (Redis Pub/Sub) and a durable log with consumer groups (Redis Streams).
The plan originally justified the split as "Pub/Sub for low latency." That
justification is weak: `XREAD BLOCK $` tails a stream in real time too, so the
latency delta is small.

The real distinguishers between the paths are:
- **Slow-consumer behavior:** Pub/Sub drops a slow subscriber (loss by design);
  a Stream lets a slow consumer lag without loss.
- **Memory:** every `XADD` costs RAM until trimmed; `PUBLISH` stores nothing.
- **Backlog for a live view:** a map wants "latest, skip the past" — Pub/Sub
  gives that free; a lagging stream reader must grind through stale entries.

## Decision
Keep **both** paths, routed by a principled rule instead of by topic name:

> **State** — the current value of something that changes continuously, re-sent
> on every tick, harmless to lose (the next message supersedes it) → **Pub/Sub**.
>
> **Event** — a discrete thing that happened once, gone forever if lost →
> **Streams** (consumer groups, `XACK`, replay).
>
> **Absence is not a message.** "Vehicle went silent" cannot be published by the
> silent vehicle. A stateful *watcher* manufactures it by timing out on missing
> state, and the resulting event travels the durable path.

Position updates (lat/lon/speed/heading/status) are state → Pub/Sub. Geofence
enter/exit, vehicle-offline (dropout), trip lifecycle → events → Streams.

Note: `status` rides on the state path even though it *sounds* like an event,
because the current status is re-sent on every position message (level-triggered,
self-healing). Only edge-triggered, fire-once facts belong on the durable path.

## Consequences
- The dual path is not strictly necessary (Streams-only would work); it is kept
  deliberately because *experiencing both delivery semantics on the same data is
  the point of the project.* Owned, not cargo-culted.
- Requires a **dropout/heartbeat watcher** consumer: `vehicle_id → last_seen`,
  timer scan, emits `vehicle_offline` events. Positions double as heartbeats.
- Every new message type must be classified state-vs-event before it gets a
  transport. This rule is the design's litmus test.
- Feeds the glossary: state vs event, heartbeat, absence detection,
  at-most-once vs at-least-once.

# ADR 0005 — Current-state snapshot (materialized read model) for cold-start

**Status:** Accepted
**Date:** 2026-07-04

## Context
Positions travel on Pub/Sub, which stores nothing and delivers only messages
published *after* a client subscribes (ADR-0002). Consequence: a **newly
connecting browser sees a blank map** that fills in vehicle-by-vehicle over ~2s,
and **never sees currently-silent vehicles** (parked / offline) because they
aren't publishing. Pub/Sub structurally cannot hand a newcomer the current state.

An event/state *stream* tells you what changed; it cannot tell a newcomer what
*is*.

## Decision
Maintain a **materialized current-state read model**: a Redis **hash**
`positions:current:{city}`, one field per `vehicle_id` holding its latest
position (+ status / last_seen). One call `HGETALL` returns the whole fleet.

- **Writer:** a small materializer consumer of the `telemetry:{city}` stream
  applies each update to the hash (overwrite the vehicle's field). Deriving it
  from the durable stream keeps a single source of truth. The dropout watcher
  (ADR-0002) writes `status=offline` / `last_seen` into the same field.
- **New-browser connect sequence:**
  1. `SUBSCRIBE positions:{city}` first (buffer incoming live deltas),
  2. `HGETALL positions:current:{city}` to paint the whole fleet instantly,
  3. apply buffered + subsequent live messages on top.
- The tiny snapshot/live race is tolerable **because positions are level-
  triggered state** (ADR-0002): any slightly-stale apply self-heals on the next
  tick. This ordering would be a bug for events; it's safe for state.

## Consequences
- Read/write split materialized: stream of updates (write path) + current-state
  hash (read model) = a small **CQRS**. Recorded as a deliberate pattern, not an
  accident.
- Fixes the dead-vehicle hole: the hash retains a silent vehicle's last position,
  so newcomers see it (greyed via the watcher's status), not missing.
- Adds a component to the plan: the **materializer / snapshot maintainer** (can
  live in the gateway or as its own consumer). Update architecture + milestones.
- Memory: the hash is O(fleet size), not O(history) — cheap and self-bounding
  (one field per vehicle), unlike the stream which needs trimming.
- Glossary: snapshot, materialized read model, CQRS, cold-start.

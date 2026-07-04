# ADR 0006 — Stream trimming, retention window, and backpressure posture

**Status:** Accepted
**Date:** 2026-07-04

## Context
A Redis Stream is an in-memory log that grows with every `XADD`; unbounded growth
OOMs Redis, so it must be trimmed (`MAXLEN` by count, or `MINID` by age; use the
approximate `~` form for cheap radix-node trimming).

Key facts:
- **Trimming removes the oldest entries regardless of whether any consumer group
  has read them.** There is no "retain until all groups ack." Entries trimmed
  off the front are gone; `XAUTOCLAIM` only recovers entries *still in the stream*
  that sit in a dead consumer's PEL.
- Therefore **retention window = MAXLEN / production_rate.** A consumer down
  longer than that window loses the entries that fell off — the ADR-0004
  "no lost events" guarantee holds only *within* the window.
- This is intrinsic to bounded logs (Kafka trims by time/size too), not a Redis
  wart. A Stream is best understood as a **bounded, time-decoupling buffer**, not
  an unbounded durable log.

Backpressure differs sharply by path, and neither reaches the producer:
- **Pub/Sub:** slow subscriber's output buffer exceeds
  `client-output-buffer-limit pubsub` → Redis **kills the connection** (load
  shedding). Fine for state/viewers (reconnect + re-snapshot).
- **Streams:** slow consumer → **lag grows** (`XINFO GROUPS`), recoverable until
  the backlog reaches the trim horizon, then **silent loss**.
- **Redis propagates no backpressure upstream** — the producer never slows for a
  slow consumer (unlike TCP flow control / Kafka producer blocking / Reactive
  Streams). Any producer throttling must be built by us.

## Decision
1. Trim with the **approximate** form. Size retention to worst-case consumer
   downtime: target ~30–60 min of history (`MAXLEN ~ 200000` at 100 msg/s, or
   `MINID ~` for an explicit time window). Document the resulting window.
2. Treat **consumer-group lag vs. the trim horizon** as the primary durability
   alarm (M10 Grafana panel). Lag approaching retention = about to lose data.
3. Accept Pub/Sub load-shedding for viewers (drop + reconnect + re-snapshot);
   accept Streams lag-then-loss past retention as a documented limit, not a bug.
4. No upstream producer throttling for now (out of scope); note it as the thing
   you'd build if you needed true end-to-end flow control.

## Consequences
- "Durable" now carries two asterisks: ≤1s on crash (ADR-0001) **and** only the
  last ~N minutes exist, only for consumers that don't lag past the horizon.
- The snapshot hash (ADR-0005) is independent of stream retention (O(fleet),
  overwrite-in-place) — cold-start survives aggressive trimming.
- New milestone/experiment: deliberately throttle a consumer, watch lag climb
  past the trim horizon in Grafana, and observe the processing gap — the
  "backpressure & trimming" demo made tangible.
- Glossary: trimming (MAXLEN/MINID/~), retention window, lag, backpressure,
  load shedding.

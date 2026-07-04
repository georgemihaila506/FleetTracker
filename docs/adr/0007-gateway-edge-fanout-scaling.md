# ADR 0007 — Gateway as edge fan-out: slow-browser isolation, scaling, conflation

**Status:** Accepted
**Date:** 2026-07-04

## Context
The WebSocket gateway is a **bridge**: one Redis subscriber upstream
(`PSUBSCRIBE positions:* alerts:*`), many browser WebSockets downstream. Two
structural facts drive its design:

- **Fan-out happens at the edge, not in Redis.** Redis delivers each message to
  the gateway once (~100 msg/s, cheap); the gateway multiplies it across N
  browsers (100 × 500 = 50k sends/s — the real load).
- **Pub/Sub subscribers multiply delivery, they don't divide work.** Running M
  gateway instances means Redis pushes the full firehose M times (each instance
  `PSUBSCRIBE`s independently and gets every message) — the opposite of a
  consumer group, which divides messages among consumers.

## Decision
1. **Slow-browser isolation.** Never fan out with a blocking sequential
   `await ws.send()` loop (head-of-line blocking: one slow phone stalls all). Give
   **each browser its own bounded outbound queue**; on overflow, drop (oldest
   frames, or disconnect → client reconnects + re-snapshots per ADR-0005). The WS
   edge is **best-effort for BOTH positions and alerts** — the durability
   boundary is Redis, not the browser (the alert's durable copy is in the stream,
   ADR-0004). Never block the fleet's live view for one laggy tab.
2. **Scale by adding gateway instances behind a load balancer.** This divides the
   *large* dimension (browser connections / edge sends) and pays only a cheap
   multiplier on the *small* one (Redis→gateway messages). Standard "fan-out at
   the edge" for live dashboards. Escape hatch if the firehose ever gets large:
   shard subscriptions by `positions:{city}` so each gateway only takes the
   cities it serves (channel granularity already supports this).
3. **Filtering:** city via channel (`positions:{city}`); bounding-box filtering
   per-browser in gateway code (can't be a channel — unbounded boxes).
4. **Batch + conflate per animation frame (~200–500ms):** coalesce a frame's
   worth of updates into one WS message; keep only the **latest** value per
   vehicle. **Conflation is legal because positions are STATE** — never conflate
   events/alerts. Fixes marker jank; reduces frame count.

## Consequences
- Gateway is horizontally scalable; Redis is not the bottleneck at this scale
  (message volume low, connection volume high — scale the connections).
- Per-connection bounded queues are a required implementation detail, not a
  nice-to-have.
- Conflation ties the wire format back to the state-vs-event spine (ADR-0002):
  the same property that made positions droppable makes them conflatable.
- Gateway owns the connect sequence (subscribe → HGETALL snapshot → live,
  ADR-0005).
- Glossary: edge fan-out, head-of-line blocking, per-connection queue,
  conflation, fan-out multiplies vs consumer-group divides.

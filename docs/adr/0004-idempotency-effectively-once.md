# ADR 0004 — Effectively-once via idempotent consumers + durable dedup state

**Status:** Accepted
**Date:** 2026-07-04

## Context
Streams + consumer groups give **at-least-once** delivery. A consumer reads with
`XREADGROUP` (entry enters its PEL — Pending Entries List), does its work, then
`XACK`s. If it crashes *after acting but before acking*, `XAUTOCLAIM` redelivers
the entry on restart and the work runs again. So the "no lost events" demo (M7)
has a twin: **duplicate events.**

Whether a duplicate is harmless (map toast) or harmful (double toll charge)
depends on the sink. We cannot rely on the sink being harmless, and Redis offers
no exactly-once delivery (the ack can always be lost after the work is done).

## Decision
Engineer **effectively-once = at-least-once delivery + idempotent processing.**

1. **Geofence emits on transitions only (edge detection).** Keep per-vehicle
   `was_inside[zone]`; emit ENTER/EXIT only when it flips. Reprocessing a
   position that doesn't change inside/outside is a no-op → duplicates swallowed.
2. **Dedup/edge state is stored in Redis, not process memory** (e.g. hash
   `geofence:inside:{city}`, one field per vehicle). It must survive the same
   crash `XAUTOCLAIM` recovers from. On restart the consumer reads state back
   from Redis rather than starting blank.
3. **Alerts carry a deterministic id** (e.g. `47:zoneA:enter:<source-stream-id>`)
   so even a downstream sink can dedupe — belt-and-suspenders for harmful sinks.
4. **Ordering of side effects:** process → update durable state → publish alert →
   `XACK`. Accept the small residual (crash between publish and ack ⇒ redelivery
   sees state already flipped ⇒ no dup; crash between state-update and publish ⇒
   handled by the deterministic-id dedupe at the sink).

## Consequences
- The state that makes geofence *stateful* (ADR-0003) is the same state that makes
  it *idempotent* — but only once it's **durable**. This ties Rounds 3 and 4
  together.
- Analytics idempotency is harder (running sums double on replay). For this
  project, accept small analytics drift on consumer crashes, OR track a
  per-vehicle "last processed stream id" and skip already-seen entries. Note as
  a known limitation; revisit if we want an idempotent-analytics milestone.
- Teaching artifact: M7 should demo BOTH "no lost events" AND "no duplicate
  alerts" — kill the consumer right after a crossing and show the alert fires
  exactly once thanks to durable `was_inside`.
- Principle recorded: exactly-once is engineered at the consumer, never received
  from the broker.

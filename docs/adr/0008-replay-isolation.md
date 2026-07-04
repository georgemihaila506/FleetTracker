# ADR 0008 — Replay isolates data from side effects (views yes, effects never)

**Status:** Accepted
**Date:** 2026-07-04

## Context
Replay reads durable history (`XRANGE` on the stream, paced client-side by
original timestamps / a speed factor) and re-emits it so the UI can re-animate
the past. It only works on the Streams path (Pub/Sub keeps no history) — a payoff
of the dual design (ADR-0002).

**The trap:** if replay re-publishes onto the *live* channels, the still-subscribed
geofence/analytics/materializer consumers reprocess it and **re-cause real side
effects** — re-fired alerts, double-counted analytics, the live map rewound for
everyone. *Replaying data replays side effects.*

**Idempotency (ADR-0004) does NOT protect against this.** Idempotency suppresses
redelivery *within one timeline*, keyed on current state. Replay re-injects an
*old* timeline; since state has legitimately moved on (vehicle long since exited),
the resurrected event reads as brand-new and fires.

## Decision
Fix by **isolation**, not idempotency:
1. Replay re-emits to a **dedicated channel** `replay:{session}`, never the live
   `positions:{city}` / `telemetry:{city}`.
2. **Only the UI (in replay mode) subscribes to `replay:{session}`.** The
   side-effecting consumers subscribe **only to live channels, never to replay**
   — so replayed data reaches eyeballs, never effect processors.
3. Pace client-side: read in `<ms>-<seq>` order, sleep `(ts - prev_ts)/speed`.
4. **Alerts become first-class durable events:** the geofence consumer `XADD`s
   each alert to a durable `alerts:{city}` stream *and* publishes it live.
   - Live toast = the `PUBLISH` (best-effort notification, ADR-0007).
   - Replay/durability = the alert **stream**, read as DATA and shown — never
     recomputed via the geofence consumer.

## Consequences
- Principle recorded: **replay safely into views/read-models (pure functions of
  the log); never into effect processors.** Event sourcing is safe precisely
  because state-rebuild-by-replay is side-effect-free; the discipline is
  suppressing effect emission during replay.
- Design change: alerts must be durably logged (`XADD` to `alerts:{city}`), not
  only `PUBLISH`ed. Map replay = re-emit telemetry + re-emit the alert log, both
  as data, to one viewer.
- Clean separation: **live path drives effects; replay path drives only views.**
- Glossary: replay, event sourcing, side-effect vs read-model, replay isolation.

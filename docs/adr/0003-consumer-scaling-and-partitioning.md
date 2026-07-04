# ADR 0003 — Consumer scaling: separate groups for fan-out, partition-by-key for load

**Status:** Accepted
**Date:** 2026-07-04

## Context
Geofence and analytics both read `telemetry:{city}`. Two levers exist:
- **More consumer groups** = fan-out; every group sees every message (different
  jobs that each need all the data).
- **More consumers in one group** = load-sharing; each message goes to exactly
  one consumer (same job, done faster).

Naively scaling a stateful consumer by adding consumers to its group **breaks
correctness**: Redis load-balances arbitrarily, so a single vehicle's
consecutive positions land on different consumers. Any computation that compares
a message to the previous message *for the same vehicle* (fence crossing =
outside→inside; distance = sum of gaps; rolling avg speed) is then split across
processes and produces wrong answers.

Key realization: geofence AND analytics are **both stateful per vehicle**. The
scaling property is not "which consumer" but **"does it need state across
messages of the same key?"**

## Decision
1. **Geofence and analytics are separate consumer groups** (fan-out). Each needs
   the full stream. ✅ (unchanged from plan)
2. **Do not scale a stateful consumer by adding consumers to its group.** At the
   project's target scale (≤200 vehicles ≈ 100 msg/s) both run **single-consumer**.
3. **If a stateful consumer ever must scale, partition by `vehicle_id`:** shard
   into `telemetry:{city}:{shard}` where `shard = hash(vehicle_id) % N`, one
   consumer per shard — i.e. re-derive Kafka-style partitions. Out of scope now;
   documented as the correct move.
4. **Stateless consumers may scale freely** (N consumers in one group): an
   archiver, forwarder, or validator that treats each message in isolation. A
   good optional milestone to *demonstrate* competing-consumers load-sharing.

## Consequences
- Correctness constraint recorded: per-vehicle ordering must be preserved for
  geofence/analytics; single-consumer guarantees it trivially.
- Gives a clean teaching contrast: run the stateless archiver as 3 competing
  consumers (works) vs. imagine geofence as 3 (breaks) — the same mechanism, two
  outcomes, decided solely by statefulness.
- Glossary: consumer group, competing consumers, partition/shard, key affinity.

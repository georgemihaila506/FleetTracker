# ADR 0001 — Redis durability posture: accept the ~1s loss window

**Status:** Accepted
**Date:** 2026-07-04

## Context
The design routes "must not lose data" traffic (geofence alerts, analytics,
replay) through Redis Streams. But Redis is **in-memory-authoritative**: Streams
live in RAM; disk (RDB snapshots / AOF) exists only to rebuild RAM after a
restart. With the default `appendfsync everysec`, a hard crash can lose up to
~1 second of `XADD`s that had not yet been fsync'd — including "durable"-path
events. Redis durability is a *dial* (`always` / `everysec` / `no`), not an
absolute.

Options weighed:
- (a) `appendfsync always` — true per-write durability, throughput cost.
- (b) Accept the ~1s window, documented, as "durable enough."
- (c) Stronger (replication / a real log like Kafka).

## Decision
**(b).** Run Redis with default `appendfsync everysec`. Accept that a crash may
drop ≤1s of stream writes. This is a personal learning project about pub/sub
*patterns*, not a system with a real durability SLA; the ~1s window is
irrelevant to the learning goals and to a demo.

"Durable" in this project therefore means **durable relative to Pub/Sub** (no
loss from slow consumers; survives a clean restart; survives consumer crashes
via pending-entry replay) — *not* zero-loss across a Redis crash.

## Consequences
- No throughput hit from per-write fsync; simpler ops.
- The geofence "no lost events" demo (kill/restart the *consumer*) still holds
  fully — that failure mode is about consumer crashes, which Streams + `XACK` +
  `XAUTOCLAIM` handle regardless of fsync policy.
- **Revisit if** the project ever grows a real durability requirement, or if we
  want to demonstrate the `always`-vs-`everysec` throughput tradeoff as its own
  experiment (a legitimately interesting one to add later).
- Glossary: pins down that Redis "durability" ≠ disk-log durability (contrast
  with the DDIA storage-engine append-only log, where disk *is* authoritative).

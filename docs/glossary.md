# Glossary — Live Fleet Tracker

Precise definitions for this project. Two vocabularies: the **business domain**
(what the app is about) and the **messaging domain** (what you're here to learn).
Entries accrue as the design firms up during grilling.

## Business domain
- **Vehicle** — a simulated delivery/ride unit emitting GPS telemetry. Has an id,
  a position, a status, and (later) a route.
- **Position update** — one telemetry sample: `{vehicle_id, lat, lon, speed,
  heading, status, ts}`. High-frequency (every 1–2s), individually disposable.
- **Geofence** — a polygon zone; crossing its boundary is an *event* worth
  reacting to (enter/exit alert).
- **Alert** — a discrete, consequential event (e.g. vehicle entered restricted
  zone). Losing one is a bug.

## Messaging domain (the learning payload)
- **State vs. event** — the project's routing litmus. *State* = current value,
  re-sent constantly, safe to lose (next msg supersedes it). *Event* = happened
  once, gone forever if lost. State → Pub/Sub; event → Streams. (ADR-0002)
- **Pub/Sub (Redis)** — `PUBLISH`/`SUBSCRIBE`. Fire-and-forget: Redis writes the
  message to current subscribers' buffers and stores *nothing*. No id, no
  history, no ack. Slow subscriber → disconnected (message lost). Zero memory.
  Delivery: **at-most-once**.
- **Stream (Redis)** — `XADD`/`XREAD`. An append-only log **in RAM** (disk only
  rebuilds RAM on restart — contrast the DDIA on-disk log where disk is
  authoritative). Entries keep a monotonic id `<ms>-<seq>` until trimmed.
- **Tailing** — `XREAD BLOCK 0 STREAMS s $`: block until entries newer than now
  arrive. Real-time, like Pub/Sub, but the entries persist.
- **At-most-once / at-least-once** — Pub/Sub delivers each message 0-or-1 times
  (may drop). Streams+consumer-groups deliver 1-or-more times (may redeliver on
  crash/timeout) — so consumers must be **idempotent**.
- **Heartbeat** — a periodic "I'm alive" signal. Here, every position update
  doubles as one.
- **Absence detection** — you cannot subscribe to silence; a messaging system
  delivers presence, never absence. A stateful **watcher** with a timeout turns
  "no heartbeat for N seconds" into a real event (vehicle-offline). (ADR-0002)
- **Durability (Redis)** — a dial (`appendfsync always`/`everysec`/`no`), not
  absolute. Default `everysec` can lose ≤1s on a crash. "Durable" here means
  *relative to Pub/Sub*, not zero-loss. (ADR-0001)

- **Consumer group** — a named cursor + bookkeeping over a stream. Each group
  reads *every* message independently. Multiple groups = **fan-out**. (ADR-0003)
- **Consumer (in a group)** — a worker. Redis gives each message to exactly one
  consumer in the group. Multiple consumers = **competing consumers** /
  load-sharing (each message processed once). (ADR-0003)
- **Fan-out vs load-share** — more *groups* = same data delivered more times;
  more *consumers in a group* = data divided up. Different job → new group; same
  job, faster → more consumers.
- **Stateful vs stateless consumer** — the property that decides scalability.
  Stateful = needs memory across messages of the same key (geofence's previous
  position, analytics' running totals) → can't naively add consumers. Stateless =
  each message independent (archiver, forwarder) → scale freely. (ADR-0003)
- **Partition / shard / key affinity** — to scale a stateful consumer you must
  route all of one key's messages to one consumer: `hash(vehicle_id) % N` into
  separate streams. This is exactly a Kafka partition. Redis Streams don't do it
  for you. (ADR-0003)

- **XACK / PEL (Pending Entries List)** — `XREADGROUP` moves an entry into the
  consumer's PEL ("delivered, not yet acked"). `XACK` clears it. Un-acked entries
  are recoverable, which is how Streams avoid loss on consumer crash. (ADR-0004)
- **XAUTOCLAIM** — reassigns entries stuck in a dead consumer's PEL (older than a
  min-idle time) to a live consumer, which reprocesses them. The crash-recovery
  mechanism behind "no lost events" — and the source of redelivery. (ADR-0004)
- **At-least-once** — a message may be delivered more than once (redelivery on
  crash/timeout). The default for Streams+groups. Requires idempotent consumers.
- **Idempotent processing** — processing a message twice == processing it once.
  For geofence: edge detection on durable per-vehicle state makes replays no-ops.
- **Effectively-once** — at-least-once delivery + idempotent processing. The real
  substitute for "exactly-once," which no broker actually delivers; it's
  engineered at the consumer, not received. (ADR-0004)
- **Durable dedup state** — the state a consumer uses to recognise a replay must
  outlive the crash it's recovering from → store it in Redis, not RAM. (ADR-0004)

- **Snapshot / current-state read model** — a Redis **hash**
  `positions:current:{city}` (one field per vehicle = latest position) that a new
  client loads with one `HGETALL` on connect. Plugs Pub/Sub's cold-start hole:
  streams say what *changed*, the snapshot says what *is*. (ADR-0005)
- **Materialized view** — a query-optimised current-state projection kept up to
  date by applying a stream of updates. Here maintained by a materializer
  consumer of `telemetry:{city}`. (ADR-0005)
- **CQRS** — separating the write path (stream of updates) from the read model
  (current-state hash). Fell out naturally from Pub/Sub having no history.
- **Cold-start** — a fresh subscriber has no history; must load a snapshot before
  live deltas make sense. Connect order: subscribe → snapshot → apply live.

- **Trimming (XTRIM / MAXLEN / MINID)** — cap a stream's size by count (`MAXLEN`)
  or age (`MINID`); use `~` for cheap approximate trimming. Removes the OLDEST
  entries **regardless of whether any consumer read them.** (ADR-0006)
- **Retention window** — `MAXLEN / production_rate` = how far back the stream
  reaches. A consumer down longer than this loses the trimmed entries; the
  durability guarantee only holds *within* the window. (ADR-0006)
- **Lag** — how far a consumer group is behind the stream tail (`XINFO GROUPS`).
  The primary durability alarm: lag approaching the trim horizon = imminent loss.
- **Backpressure** — how "consumer can't keep up" propagates. Pub/Sub: drop the
  slow subscriber (**load shedding**). Streams: grow lag, then lose at the trim
  edge. Redis pushes **no** backpressure upstream to the producer. (ADR-0006)
- **Bounded time-decoupling buffer** — the honest model of a Redis Stream: lets a
  consumer fall behind the producer, but only by a bounded amount (the window).

- **Edge fan-out** — the expensive fan-out happens at the gateway (1 Redis msg →
  N browser sends), not in Redis. Scale the gateway (many connections), not the
  broker (few messages). (ADR-0007)
- **Subscribers multiply, groups divide** — adding Pub/Sub subscriber *processes*
  makes Redis deliver the firehose to each (multiplies); adding consumers to a
  *group* splits messages among them (divides). The core scaling distinction.
- **Head-of-line blocking** — a sequential fan-out loop stalls all browsers
  behind one slow one. Fix: per-connection **bounded outbound queue**, drop on
  overflow. WS edge is best-effort; durability boundary is Redis. (ADR-0007)
- **Conflation** — keep only the newest value per key, drop intermediates (e.g.
  batch a frame's positions to latest-per-vehicle). Legal for **state**, never
  for events. The wire-format face of state-vs-event. (ADR-0007)

- **Replay** — re-emitting durable history (`XRANGE`, paced client-side by
  original timestamps / speed) so the UI can re-animate the past. Only possible
  on the Streams path; Pub/Sub has no history. (ADR-0008)
- **Event sourcing** — keeping the log of everything that happened so any past
  state can be reconstructed by replaying it. Safe for state-rebuild; dangerous
  for effect emission.
- **Replay isolation** — replay re-emits to a dedicated `replay:` channel only
  the UI reads; side-effecting consumers never subscribe to it, so replaying data
  never re-fires alerts/analytics. Idempotency does NOT solve this (it guards one
  timeline; replay injects an old one). (ADR-0008)
- **Read model vs. effect processor** — you can safely replay into anything that
  only builds state/views (pure function of the log); never into anything with
  external side effects. (ADR-0008)

## Mental-model clarifications (things that get fuzzy)

- **The fan-out tree (not a line)** — the pipeline isn't
  `simulator → gateway → browser`. One producer publishes to *one* channel, and
  **N independent subscribers each get every message**:

  ```
                     ┌─SUBSCRIBE─► gateway ──WebSocket──► browsers
  simulator ─PUBLISH─► positions:{city}
     (state)          └─SUBSCRIBE─► materializer ─HSET─► positions:current:{city}
                        (Pub/Sub)                             (snapshot hash)
  ```

  Adding the materializer took nothing away from the gateway — Pub/Sub copies each
  message to both. That's what lets more consumers (geofence, analytics, dropout)
  bolt on later as just one more `SUBSCRIBE`. The two subscribers do *opposite*
  things with the same feed: the gateway does **transient** delivery (push and
  forget), the materializer does **stateful** accumulation (fold into a hash).
  (ADR-0003)

- **"streaming" (lowercase) vs. Redis Streams (capital S)** — a name collision
  worth pinning:
  - *streaming* = continuous push of messages. The **WebSocket** streams to the
    browser; **Pub/Sub** streams server-side. Both are "streaming" in this loose
    sense.
  - **Redis Streams** (`XADD`) = a specific **durable log** data structure for
    *events*, not built until M6/M7. Unrelated to WebSockets.
  - So "the WebSocket is the streaming part" is fine *if* you mean the browser
    transport — but a WebSocket is best-effort / at-most-once, whereas Redis
    Streams will be durable / at-least-once. Different layer, different guarantee.
    (ADR-0002)

- **The three hops** — data streams across three different transports:
  `simulator ─PUBLISH─► Pub/Sub` (hop 1), `Pub/Sub ─SUBSCRIBE─► gateway` (hop 2,
  server-side stream), `gateway ─WebSocket─► browser` (hop 3, client-side stream).
  The gateway is the bridge that turns one server-side stream into N client
  streams — the fan-out. (ADR-0007)

<!-- Decisions that pin these terms down are recorded in docs/adr/. -->

# Live Fleet Tracker — Project Plan

A real-time delivery/ride-sharing fleet tracker built around **Redis Pub/Sub + Streams** and **Python**. Simulated vehicles publish GPS telemetry; multiple independent consumers process it; a live map shows everything in real time.

> **This plan has been hardened by a design grilling** (see `docs/adr/0001`–`0008`
> and `docs/glossary.md`). The whole design hangs on one distinction —
> **state vs. event** — which decides transport, scaling, idempotency, cold-start,
> retention, wire format, and replay safety. Read the ADRs for the *why*; this
> plan is the *what*.

## Goals

- Learn pub/sub patterns hands-on: fan-out, consumer groups, replay, backpressure
- Contrast ephemeral messaging (Redis Pub/Sub) with durable logs (Redis Streams)
- Ship a demo-able live map you can show off

## The one idea everything hangs on (ADR-0002)

> **State** = the current value of something that changes continuously, re-sent on
> every tick, safe to lose (the next message supersedes it) → **Pub/Sub**.
> **Event** = a discrete thing that happened once, gone forever if lost → **Streams**.
> **Absence is not a message** — a silent/dead vehicle publishes nothing, so a
> stateful *watcher* must manufacture "vehicle offline" by timing out on missing state.

Positions are state (→ Pub/Sub). Geofence crossings, vehicle-offline, trip
lifecycle are events (→ Streams). `status` rides the state path because it's
re-sent on every position message (level-triggered, self-healing).

## Architecture

```
                        ┌────────────────────────────────────────────────┐
                        │                     Redis                       │
 ┌──────────────┐       │                                                 │
 │  Vehicle     │──────▶│  Stream:  telemetry:{city}   (durable, XADD)    │
 │  Simulator   │       │  Pub/Sub: positions:{city}   (ephemeral state)  │
 │  (asyncio)   │       │                                                 │
 └──────────────┘       └──┬────────┬──────────┬──────────┬──────────┬────┘
                           │        │          │          │          │
                  consumer groups (Streams, fan-out)      │          │ pub/sub
                           │        │          │          │          │ state
                  ┌────────▼─┐ ┌────▼─────┐ ┌──▼───────┐ ┌▼────────┐  │
                  │ Geofence │ │Analytics │ │Dropout   │ │Material-│  │
                  │ consumer │ │ consumer │ │ watcher  │ │ izer    │  │
                  │(idempot.)│ │(per-veh.)│ │(timeout) │ │(snapshot)│ │
                  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬────┘  │
                       │            │            │            │       │
              XADD alerts:{city}  metrics   XADD alerts   HSET        │
              + PUBLISH alert    (hashes)   (offline)   positions:    │
                       │                                current:{city}│
                       │                                     │        │
                       └───────────────┐        ┌────────────┘        │
                                       ▼        ▼                     ▼
                                  ┌─────────────────────────────────────┐
                                  │  WebSocket gateway (FastAPI)         │
                                  │  connect: HGETALL snapshot → then    │
                                  │  live Pub/Sub; per-conn bounded      │
                                  │  queue; batch+conflate per frame     │
                                  └───────────────┬─────────────────────┘
                                                  │ ws://
                                                  ▼
                                          Live map UI (Leaflet.js)
```

**Why both Pub/Sub and Streams (ADR-0002):** *not* "Pub/Sub is faster" — `XREAD
BLOCK $` tails a stream in real time too. The real reasons: (1) slow-consumer
behavior — Pub/Sub drops a slow subscriber (loss by design, fine for viewers),
Streams let a consumer lag without loss; (2) memory — `XADD` costs RAM until
trimmed, `PUBLISH` stores nothing; (3) a live view wants "latest, skip the
backlog," which Pub/Sub gives free. Keeping both is deliberate: **experiencing
both delivery semantics on the same data is the point of the project.**

**Durability posture (ADR-0001):** Redis is RAM-authoritative (disk only rebuilds
RAM on restart). We run default `appendfsync everysec` and accept a ≤1s crash-loss
window. "Durable" here means *relative to Pub/Sub* — no loss from slow consumers,
survives consumer crashes via pending-entry replay — not zero-loss.

## Components

### 1. Vehicle simulator (producer)
- `asyncio` workers, one per vehicle (target: 50–200 vehicles)
- Each vehicle follows a route (OSRM public API or precomputed polylines), emits every 1–2s: `{vehicle_id, lat, lon, speed, heading, status, ts}`
- Vehicle state machine: `idle → assigned → en_route → delivering → idle`
- Writes each update with `XADD` to `telemetry:{city}` **and** `PUBLISH` to `positions:{city}`. Positions double as **heartbeats** (feed the dropout watcher).
- Trims on write: `XADD telemetry:{city} MAXLEN ~ <N> *` (retention window sized to worst-case consumer downtime — ADR-0006)
- Configurable: vehicle count, tick rate, chaos flags (dropouts, GPS jitter)

### 2. WebSocket gateway (FastAPI) — edge fan-out (ADR-0005, 0007)
- Subscribes to `positions:*` and `alerts:*` via Pub/Sub, fans out to browser WebSocket clients
- **Connect sequence:** `SUBSCRIBE` → `HGETALL positions:current:{city}` (snapshot) → apply live deltas. Fixes the cold-start blank map; the tiny race self-heals because positions are state.
- **Slow-browser isolation:** per-connection **bounded outbound queue**; on overflow drop/disconnect (client reconnects + re-snapshots). Never a blocking sequential fan-out loop (head-of-line blocking).
- **Batch + conflate per animation frame** (~200–500ms): coalesce a frame's updates, keep only the latest per vehicle. *Conflation is legal because positions are state.*
- Scales horizontally behind a load balancer: message volume is tiny (subscribers multiply the firehose, but it's cheap), connection volume is large and gets divided.
- Per-connection subscriptions (client picks a city / bounding box; city via channel, bbox filtered in-gateway)
- REST endpoints: fleet snapshot, vehicle detail, active alerts, metrics

### 3. Geofence consumer — idempotent (ADR-0003, 0004)
- Stream consumer group `geofence` on `telemetry:{city}`; **single consumer** (stateful per vehicle → can't naively add consumers; shard by `vehicle_id` if ever needed)
- Zones as polygons (Shapely); **edge detection** on per-vehicle `was_inside` state stored **in Redis** (`geofence:inside:{city}`, survives consumer crash) → emits ENTER/EXIT only on transitions ⇒ **idempotent**, replays are no-ops
- Emits alerts to a **durable stream** `XADD alerts:{city}` *and* `PUBLISH`es for live toast (durable copy enables replay; publish is best-effort notification)
- Demonstrates: consumer groups, `XACK`, `XAUTOCLAIM` for crash recovery, **effectively-once = at-least-once + idempotency**
- Alerts stamped with a deterministic id (`veh:zone:enter:<source-id>`) for sink-side dedupe

### 4. Analytics consumer (ADR-0003)
- Separate consumer group `analytics` on the same stream — fan-out, independent cursor
- **Stateful per vehicle** (distance = sum of gaps, rolling avg speed) → single consumer
- Computes: per-vehicle distance/avg speed, fleet utilization, simple ETA; stores rollups in Redis hashes; exposes via the gateway
- Known limitation: not idempotent across crashes (running sums double on replay) — accept small drift, or track a per-vehicle last-processed id (revisit later)

### 5. Materializer / snapshot maintainer (NEW — ADR-0005)
- Consumes `telemetry:{city}`, keeps the **current-state hash** `positions:current:{city}` (one field per vehicle = latest position + status/last_seen) up to date
- This is the read model the gateway loads on cold-start. A small **CQRS**: stream of updates (write path) + current-state hash (read model). Independent of stream trimming (O(fleet), overwrite-in-place)

### 6. Dropout / heartbeat watcher (NEW — ADR-0002)
- Keeps `vehicle_id → last_seen`; refreshed by every position update
- Timer scan (~5s): any vehicle silent > threshold → `XADD` a `vehicle_offline` event and mark `status=offline` in the snapshot hash (map greys it out)
- Embodies "absence is not a message — a watcher manufactures it by timing out"

### 7. Replay tool — isolated (ADR-0008)
- CLI: `replay --from <ts> --speed 10x` — reads history with `XRANGE`, paces client-side by original timestamps/speed, republishes to a **dedicated `replay:{session}` channel only the UI reads**
- Side-effecting consumers **never** subscribe to `replay:` → replaying data never re-fires alerts/analytics (idempotency does NOT solve this: it guards one timeline, replay injects an old one)
- Shows historical alerts by re-emitting the durable `alerts:{city}` log as **data** — never by recomputing via the geofence consumer
- Shows off why durable logs matter (event sourcing); replay is a Streams-only capability

### 8. Live map UI
- Single-page app: Leaflet + vanilla JS (or small Vue), served by FastAPI
- Animated vehicle markers, status colors (incl. grey = offline), geofence polygons, alert toasts, fleet stats panel
- Replay mode: switch to the `replay:` channel

## Milestones

**M1 — Skeleton & broker**
Repo scaffold, docker-compose with Redis, shared pydantic models, config, redis helper module. `redis-cli MONITOR` shows a hand-published test message.

**M2 — Simulator publishes**
Simulator with 5 vehicles on hardcoded routes, `PUBLISH` to `positions:{city}` every 1–2s. Verify with `SUBSCRIBE` in redis-cli.

**M3 — Gateway & WebSocket fan-out**
FastAPI gateway subscribes to `positions:*`, fans out to browser WebSocket clients with **per-connection bounded queues** (no head-of-line blocking). Browser console logs live positions. Fleet snapshot REST endpoint.

**M4 — Live map + cold-start snapshot**
Leaflet UI with animated markers, status colors. Served by the gateway. **Materializer maintains `positions:current:{city}`; gateway connect = snapshot → live** so a fresh browser sees the whole fleet instantly (ADR-0005). Batch+conflate per frame.

**M5 — Real routes & scale**
OSRM routes (cached locally), vehicle state machine, scale to 50+ vehicles. Client-side interpolation if markers get janky.

**M6 — Durable stream path + retention**
Add `XADD` to `telemetry:{city}` alongside publish. **Trim with `MAXLEN ~`; document retention window = MAXLEN/rate** (ADR-0006). Inspect with `XRANGE`/`XLEN`.

**M7 — Geofence consumer (the money demo)**
Consumer group with `XACK` + `XAUTOCLAIM`. Zone polygons (Shapely), **edge detection on durable `was_inside`**; alerts `XADD`'d to `alerts:{city}` + published; toasts on map. **Kill/restart the consumer mid-run and verify BOTH: (a) no lost events AND (b) no duplicate alerts** — effectively-once (ADR-0004).

**M8 — Analytics consumer**
Second consumer group on the same stream (independent cursor, single consumer). Distance, avg speed, utilization, simple ETA. Stats panel in UI.

**M9 — Dropout watcher**
`last_seen` tracking + timer scan; emit `vehicle_offline`, grey the marker (ADR-0002). Chaos flag: kill a vehicle's telemetry, watch it go offline after the threshold.

**M10 — Replay (isolated)**
CLI `replay --from <ts> --speed 10x` over `XRANGE`, republish to a dedicated `replay:` channel the UI reads; **consumers never subscribe to it** (ADR-0008). Replay re-emits telemetry + the durable alert log as data. UI replay mode.

**M11 — Grafana observability**
Grafana + Prometheus + `redis_exporter` in docker-compose. Dashboards: messages/sec per channel, stream length, **consumer-group lag vs. trim horizon (the durability alarm — ADR-0006)**, gateway WS connections, end-to-end latency (publish ts → browser receive). App metrics via `prometheus-fastapi-instrumentator` and a small exporter in each consumer.

**M12 — Locust load testing**
Locust scenarios: (a) ramp simulator to 500–1000 vehicles, (b) ramp WebSocket clients against the gateway. Measure p50/p95 end-to-end latency and dropped-message rate under load; watch it live in Grafana. **Backpressure demo:** throttle a consumer, watch lag climb past the trim horizon, observe the processing gap (ADR-0006). Document findings in README.

**M13 — Polish**
Chaos flags demo (dropouts, GPS jitter), README with architecture diagram, demo GIF, "lessons learned" write-up. Optional: a stateless archiver run as 3 competing consumers to *demonstrate* load-sharing (contrast with why geofence can't — ADR-0003).

## Repo layout

```
FleetTracker/
├── docker-compose.yml        # redis, gateway, simulator, consumers
├── simulator/                # producer package
├── gateway/                  # FastAPI + websocket fan-out + static UI
├── consumers/
│   ├── geofence.py           # idempotent, durable was_inside
│   ├── analytics.py
│   ├── materializer.py       # maintains positions:current:{city}
│   └── dropout.py            # heartbeat/absence watcher
├── replay/
├── shared/                   # models (pydantic), redis helpers, config
├── observability/            # prometheus.yml, grafana dashboards (JSON)
├── loadtest/                 # locustfiles
├── docs/
│   ├── adr/                  # 0001–0008 design decisions
│   └── glossary.md
└── tests/
```

## Stack

Python 3.12, `redis-py` (asyncio), FastAPI + uvicorn, pydantic, Shapely, Leaflet.js, Docker Compose, pytest, Grafana + Prometheus + redis_exporter (observability), Locust (load testing).

## Design decisions (ADRs)

Full rationale in `docs/adr/`:
- **0001** — Redis durability posture: accept the ~1s loss window
- **0002** — Two messaging paths, split by state vs. event (the spine)
- **0003** — Consumer scaling: groups fan-out, partition-by-key for load
- **0004** — Effectively-once via idempotent consumers + durable dedup state
- **0005** — Current-state snapshot (materialized read model) for cold-start
- **0006** — Trimming, retention window, and backpressure posture
- **0007** — Gateway as edge fan-out: slow-browser isolation, scaling, conflation
- **0008** — Replay isolates data from side effects (views yes, effects never)

## Key pub/sub concepts you'll exercise

| Concept | Where |
|---|---|
| Fan-out to many subscribers | positions → N browser clients (edge fan-out) |
| At-most-once vs at-least-once | Pub/Sub path vs Streams path |
| Consumer groups & partitioned work | geofence + analytics on same stream |
| Ack, pending entries, crash recovery | `XACK` / `XAUTOCLAIM` in geofence consumer |
| Idempotency / effectively-once | edge detection on durable `was_inside` |
| Absence detection | dropout watcher (timeout on missing heartbeats) |
| Materialized read model / CQRS | `positions:current:{city}` snapshot hash |
| Replay / event sourcing | replay CLI over `XRANGE`, isolated channel |
| Topic hierarchies | `positions:{city}`, `PSUBSCRIBE positions:*` |
| Backpressure & trimming | slow-consumer test, lag vs trim horizon, `XTRIM` |
| Conflation | batch+latest-per-vehicle to the browser (state only) |

## Risks / decisions to make later

- **Marker animation jank** at 100+ vehicles → batch+conflate WS messages per frame (ADR-0007), interpolate client-side
- **Route data**: OSRM public API is rate-limited → cache routes locally, or ship precomputed GeoJSON
- **Redis Pub/Sub drops messages for slow clients** → that's a feature here; document it (ADR-0006, 0007)
- **Scaling a stateful consumer** → shard by `vehicle_id` (Kafka-style partitions); out of scope now (ADR-0003)
- **Analytics idempotency** across crashes → accept drift or track last-processed id (ADR-0004)
- If you outgrow Redis, the architecture maps 1:1 onto Kafka/NATS — a good future write-up

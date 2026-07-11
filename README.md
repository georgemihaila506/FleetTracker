# Live Fleet Tracker

A real-time delivery/fleet map built on **Redis Pub/Sub + Streams** in Python — a
personal, hands-on project for learning pub/sub patterns (fan-out, consumer
groups, idempotency, backpressure, replay). Simulated vehicles publish GPS
telemetry; independent consumers process it; a live Leaflet map shows everything.

## The one idea

**State vs. event.** Current-value data that's re-sent every tick and safe to lose
→ **Pub/Sub** (positions). Discrete things that happened once and must not be lost
→ **Streams** (geofence crossings, offline events). That single distinction drives
transport, scaling, idempotency, cold-start, retention, wire format, and replay.

- **Plan:** [`PLAN.md`](PLAN.md) — components, milestones (M1–M13)
- **Design decisions:** [`docs/adr/`](docs/adr/) — 0001–0008, the *why*
- **Glossary:** [`docs/glossary.md`](docs/glossary.md) — pub/sub terms from first principles

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate            # Windows (PowerShell: .venv\Scripts\Activate.ps1)
pip install -e ".[dev]"           # editable install + test deps
```

Redis runs via Docker Compose (added in M1):

```bash
docker compose up -d redis
```

## Layout

`src/` layout; components are subpackages of `fleet_tracker` (added per milestone).

```
src/fleet_tracker/  shared/ simulator/ gateway/ consumers/ replay/
docs/               adr/ (0001-0008)  glossary.md
tests/
```

## Status

Design complete (8 ADRs). Done: **M1** (skeleton & broker), **M2** (simulator →
Pub/Sub), **M3** (gateway WebSocket fan-out), **M4** (live Leaflet map +
materializer/cold-start snapshot), **M5** (real OSRM road routes, 50 vehicles),
**M6** (durable stream path: `XADD telemetry:{city}` + `MAXLEN ~` retention),
**M7** (geofence consumer group — edge-detection idempotency, XACK/XAUTOCLAIM,
effectively-once; zone polygons + alert toasts on the map),
**M8** (analytics consumer — second group / independent cursor, cumulative
distance + avg speed, fleet stats panel),
**M9** (dropout watcher — absence detection by heartbeat timeout; greys offline
vehicles on the map).
Next: **M10** (replay — isolated `XRANGE` re-emit).

"""Fetch real road routes from OSRM once and cache them in the repo.

M5 swaps the simulator's random walk for vehicles that follow actual streets.
Rather than call the (rate-limited) public OSRM demo server at runtime, we call
it *once* here and commit the result. The simulator then reads the cached
`routes.json` — no network dependency, no rate limits, reproducible.

Each route is stored as a plain list of ``[lat, lon]`` points tracing a drivable
path between two random points in Bucharest. OSRM returns GeoJSON coordinates as
``[lon, lat]``; we flip them to ``[lat, lon]`` here so everything downstream
(Leaflet, the Position model) speaks the same order.

Run (from the repo root, with the venv interpreter):

    .venv/Scripts/python.exe scripts/fetch_routes.py

Re-run only when you want fresh routes; the committed cache is the source of truth.
"""

from __future__ import annotations

import json
import random
import time
import urllib.request
from pathlib import Path

# Public OSRM demo server. Fine for a handful of one-off requests; do not hammer.
_OSRM = "http://router.project-osrm.org/route/v1/driving"

# Bucharest bounding box (roughly the city proper). Random endpoints land on real
# streets because OSRM snaps to the nearest road.
_LAT_MIN, _LAT_MAX = 44.39, 44.47
_LON_MIN, _LON_MAX = 26.04, 26.17

_ROUTE_COUNT = 20           # how many distinct routes to cache
_MIN_POINTS = 15            # skip degenerate near-zero routes
_OUT = Path(__file__).resolve().parents[1] / "src" / "fleet_tracker" / "simulator" / "data" / "routes.json"


def _random_point() -> tuple[float, float]:
    return random.uniform(_LAT_MIN, _LAT_MAX), random.uniform(_LON_MIN, _LON_MAX)


def _fetch_one() -> list[list[float]] | None:
    """Ask OSRM for one driving route between two random points; return [lat, lon]s."""
    (lat1, lon1), (lat2, lon2) = _random_point(), _random_point()
    url = (
        f"{_OSRM}/{lon1:.6f},{lat1:.6f};{lon2:.6f},{lat2:.6f}"
        "?overview=full&geometries=geojson"
    )
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = json.load(resp)
    if data.get("code") != "Ok" or not data.get("routes"):
        return None
    coords = data["routes"][0]["geometry"]["coordinates"]  # [lon, lat] pairs
    if len(coords) < _MIN_POINTS:
        return None
    return [[lat, lon] for lon, lat in coords]  # flip to [lat, lon]


def main() -> None:
    random.seed(20260711)  # reproducible set of routes
    routes: list[list[list[float]]] = []
    attempts = 0
    while len(routes) < _ROUTE_COUNT and attempts < _ROUTE_COUNT * 4:
        attempts += 1
        try:
            route = _fetch_one()
        except Exception as exc:  # network hiccup — retry with a fresh pair
            print(f"  attempt {attempts}: {exc!r} (retrying)")
            time.sleep(1.0)
            continue
        if route is None:
            continue
        routes.append(route)
        print(f"  route {len(routes):2d}/{_ROUTE_COUNT}: {len(route)} points")
        time.sleep(0.4)  # be polite to the demo server

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    _OUT.write_text(json.dumps(routes), encoding="utf-8")
    total = sum(len(r) for r in routes)
    print(f"\nwrote {len(routes)} routes ({total} points) -> {_OUT}")


if __name__ == "__main__":
    main()

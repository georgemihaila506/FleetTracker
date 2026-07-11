"""Road routes for the simulator — loading + geometry helpers (DONE for you).

A `Route` is a polyline: an ordered list of ``(lat, lon)`` points tracing a real
street path (fetched from OSRM by ``scripts/fetch_routes.py`` and cached in
``data/routes.json``). To move a vehicle *along* such a path at a given speed you
need one more thing precomputed: how many metres each point sits from the start.
That's ``cum`` — the cumulative distance array — built here once at load time so
the follower can turn "I've driven 850 m" into "...so I'm between points 42 and
43" with a cheap binary search instead of re-summing the whole path every tick.

Nothing here touches Redis. This module just parses JSON and does spherical
geometry; ``route_vehicle.py`` is where a vehicle rides one of these.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

_DATA = Path(__file__).parent / "data" / "routes.json"

_EARTH_R_M = 6_371_000.0  # mean Earth radius, metres


def haversine_m(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance in metres between two ``(lat, lon)`` points."""
    lat1, lon1 = math.radians(a[0]), math.radians(a[1])
    lat2, lon2 = math.radians(b[0]), math.radians(b[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * _EARTH_R_M * math.asin(math.sqrt(h))


def bearing_deg(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Compass heading in degrees (0=N, 90=E) travelling from point ``a`` to ``b``."""
    lat1, lat2 = math.radians(a[0]), math.radians(b[0])
    dlon = math.radians(b[1] - a[1])
    y = math.sin(dlon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
    return math.degrees(math.atan2(y, x)) % 360


@dataclass
class Route:
    """A drivable polyline plus the cumulative metres to each of its points."""

    points: list[tuple[float, float]]  # (lat, lon), in order
    cum: list[float]                   # cum[i] = metres from points[0] to points[i]

    @property
    def length_m(self) -> float:
        return self.cum[-1]

    @classmethod
    def from_points(cls, points: list[tuple[float, float]]) -> "Route":
        cum = [0.0]
        for prev, cur in zip(points, points[1:]):
            cum.append(cum[-1] + haversine_m(prev, cur))
        return cls(points=points, cum=cum)


def load_routes(path: Path | None = None) -> list[Route]:
    """Load the cached OSRM routes, dropping any too short to drive along."""
    raw = json.loads((path or _DATA).read_text(encoding="utf-8"))
    routes = [Route.from_points([(lat, lon) for lat, lon in pts]) for pts in raw]
    routes = [r for r in routes if r.length_m > 0]
    if not routes:
        raise RuntimeError(
            f"no usable routes in {path or _DATA}; run scripts/fetch_routes.py"
        )
    return routes

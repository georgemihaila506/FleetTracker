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
from dataclasses import dataclass
from pathlib import Path

# Distance/bearing math now lives in shared/geo.py (analytics needs it too).
# Re-exported here so existing imports (`from .routes import haversine_m`) keep working.
from ..shared.geo import bearing_deg, haversine_m

__all__ = ["Route", "load_routes", "haversine_m", "bearing_deg"]

_DATA = Path(__file__).parent / "data" / "routes.json"


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

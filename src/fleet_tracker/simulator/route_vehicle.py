"""A vehicle that follows a real road route — the M5 motion model.

This replaces the M2 random walk (``vehicle.py``). Instead of wandering with
small random turns, a ``RouteVehicle`` is pinned to one `Route` (a polyline of
real streets) and simply advances *along* it: every tick it travels
``speed * dt`` more metres down the path and loops back to the start at the end.
Same outputs as before — ``.lat`` / ``.lon`` / ``.speed`` / ``.heading`` — so the
publish loop in ``run.py`` doesn't change at all. Only *how the dot moves* changes.

Everything here is DONE except one method: ``_locate`` — the heart of M5.

Nothing here touches Redis or pydantic; it's pure geometry over a `Route`.
"""

from __future__ import annotations

import bisect
import random
from dataclasses import dataclass, field

from .routes import Route, bearing_deg, load_routes


@dataclass
class RouteVehicle:
    vehicle_id: str
    route: Route
    distance: float  # metres travelled from the route's start (wraps at the end)
    speed: float = field(default_factory=lambda: random.uniform(6, 14))  # m/s

    # Derived each tick from `distance` — filled by _locate() below.
    lat: float = 0.0
    lon: float = 0.0
    heading: float = 0.0

    def __post_init__(self) -> None:
        # Snap to the starting position so the first published frame is correct.
        self.lat, self.lon, self.heading = self._locate(self.distance)

    def step(self, dt: float) -> None:
        """Advance the vehicle by ``dt`` seconds of driving."""
        # Vary speed a little for organic motion, clamped to a city range.
        self.speed = _clamp(self.speed + random.uniform(-1.5, 1.5), 4.0, 18.0)
        # Drive forward along the path; wrap around at the end so it loops forever.
        self.distance = (self.distance + self.speed * dt) % self.route.length_m
        self.lat, self.lon, self.heading = self._locate(self.distance)

    def _locate(self, distance: float) -> tuple[float, float, float]:
        """Map ``distance`` travelled → ``(lat, lon, heading)``.

        The route is two parallel lists — ``route.points`` (the ``(lat, lon)``
        corners of the polyline) and ``route.cum`` (``cum[i]`` = metres from
        the start to point ``i``, ascending, ``cum[0] == 0``). We locate the
        vehicle in three steps:

          1. **Which segment?** ``bisect_right(cum, distance) - 1`` is the largest
             ``i`` with ``cum[i] <= distance`` — an O(log n) lookup, the whole
             reason ``cum`` is precomputed. Clamped to ``[0, len - 2]`` so both
             ``points[i]`` and ``points[i + 1]`` exist (also handles the wrapped
             ``distance == 0`` and the final-point edge cases).
          2. **Where in it?** ``frac`` is how far between the two endpoints we are
             (0 at ``points[i]``, 1 at ``points[i + 1]``); linearly interpolate the
             lat and lon. A zero-length segment (duplicate points) → ``frac = 0``.
          3. **Heading** is the compass bearing of that segment.

        Positions are level-triggered state, so a slightly stale interpolation is
        self-correcting — the next tick re-derives from the true ``distance``.
        """
        cum = self.route.cum
        points = self.route.points

        # 1. Which segment? bisect_right gives the insertion point to the right of
        #    equal values, so `pos - 1` is the largest i with cum[i] <= distance.
        #    Clamp into [0, len - 2] so points[i] and points[i + 1] both exist.
        i = bisect.bisect_right(cum, distance) - 1
        i = max(0, min(i, len(points) - 2))

        a, b = points[i], points[i + 1]

        # 2. How far between a and b are we? Guard a zero-length segment.
        seg = cum[i + 1] - cum[i]
        frac = (distance - cum[i]) / seg if seg > 0 else 0.0
        lat = a[0] + (b[0] - a[0]) * frac
        lon = a[1] + (b[1] - a[1]) * frac

        # 3. Heading is the bearing of the segment we're driving down.
        heading = bearing_deg(a, b)

        return lat, lon, heading


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def make_route_fleet(
    count: int, routes: list[Route] | None = None
) -> list[RouteVehicle]:
    """Create ``count`` vehicles spread across the available routes.

    Routes are handed out round-robin, each vehicle dropped at a random point
    along its route (random start ``distance``) so they don't all bunch up at one
    end. More vehicles than routes just means several share a road — fine.
    """
    routes = routes or load_routes()
    fleet: list[RouteVehicle] = []
    for i in range(count):
        route = routes[i % len(routes)]
        fleet.append(
            RouteVehicle(
                vehicle_id=f"veh-{i:03d}",
                route=route,
                distance=random.uniform(0, route.length_m),
            )
        )
    return fleet

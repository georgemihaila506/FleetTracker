"""Small geographic helpers shared across components.

``haversine_m`` (great-circle distance) started life in the simulator's route
code; the analytics consumer needs the same math to sum how far a vehicle has
travelled, so it lives here in ``shared`` where both can import it without one
component depending on another.
"""

from __future__ import annotations

import math

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

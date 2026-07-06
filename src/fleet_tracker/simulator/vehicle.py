"""Vehicle movement — the model behind each simulated dot on the map.

This is DONE for you: it produces plausible-looking motion so the publish loop
you write has something real to send. A vehicle holds its current lat/lon and a
heading, and on each ``step()`` nudges itself forward with a small random turn —
a "random walk" that looks like a car wandering the streets rather than
teleporting. It stays loosely near the city centre so dots don't drift off-map.

Nothing here touches Redis or pydantic; ``step()`` just mutates state and the
loop reads ``.lat`` / ``.lon`` / ``.speed`` / ``.heading`` to build a Position.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field

# ~metres per degree of latitude (constant enough for a city-sized area).
_METRES_PER_DEG_LAT = 111_320.0


@dataclass
class Vehicle:
    vehicle_id: str
    lat: float
    lon: float
    heading: float = field(default_factory=lambda: random.uniform(0, 360))
    speed: float = field(default_factory=lambda: random.uniform(5, 15))  # m/s

    # The centre it wanders around, and how far before it's gently steered back.
    home_lat: float = 0.0
    home_lon: float = 0.0
    leash_m: float = 2_000.0

    def step(self, dt: float) -> None:
        """Advance the vehicle by ``dt`` seconds of motion."""
        # Randomly turn a little and vary speed — organic, not a straight line.
        self.heading = (self.heading + random.uniform(-25, 25)) % 360
        self.speed = _clamp(self.speed + random.uniform(-2, 2), 3, 20)

        # If we've wandered past the leash, steer roughly back toward home.
        if self._distance_from_home() > self.leash_m:
            self.heading = self._bearing_to_home()

        # Move: convert speed*dt (metres) into lat/lon deltas.
        dist_m = self.speed * dt
        rad = math.radians(self.heading)
        dlat = (dist_m * math.cos(rad)) / _METRES_PER_DEG_LAT
        dlon = (dist_m * math.sin(rad)) / (
            _METRES_PER_DEG_LAT * math.cos(math.radians(self.lat))
        )
        self.lat += dlat
        self.lon += dlon

    def _distance_from_home(self) -> float:
        dlat = (self.lat - self.home_lat) * _METRES_PER_DEG_LAT
        dlon = (
            (self.lon - self.home_lon)
            * _METRES_PER_DEG_LAT
            * math.cos(math.radians(self.lat))
        )
        return math.hypot(dlat, dlon)

    def _bearing_to_home(self) -> float:
        dlat = self.home_lat - self.lat
        dlon = self.home_lon - self.lon
        return math.degrees(math.atan2(dlon, dlat)) % 360


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def make_fleet(count: int, center_lat: float, center_lon: float) -> list[Vehicle]:
    """Create ``count`` vehicles scattered around a city centre."""
    fleet: list[Vehicle] = []
    for i in range(count):
        # Start within ~500 m of the centre.
        jlat = center_lat + random.uniform(-0.005, 0.005)
        jlon = center_lon + random.uniform(-0.005, 0.005)
        fleet.append(
            Vehicle(
                vehicle_id=f"veh-{i:03d}",
                lat=jlat,
                lon=jlon,
                home_lat=center_lat,
                home_lon=center_lon,
            )
        )
    return fleet

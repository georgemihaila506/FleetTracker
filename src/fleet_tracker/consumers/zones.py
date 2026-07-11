"""Geofence zones + point-in-polygon test (DONE for you).

M7 watches for vehicles crossing zone boundaries. A `Zone` is a polygon over the
map — an ordered list of ``(lat, lon)`` corners. The only question the geofence
consumer ever asks this module is "which zones is this point inside right now?",
answered by ``zones_containing``.

The containment test is the classic **ray casting** algorithm: shoot a horizontal
ray from the point and count how many polygon edges it crosses — odd = inside,
even = outside. ~10 lines of stdlib, no Shapely / GEOS dependency (our zones are
simple polygons, so this is plenty). Swap in Shapely later if zones get complex.

Coordinates are ``(lat, lon)`` throughout to match the rest of the app; inside the
math ``lat`` plays the role of y and ``lon`` of x.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Zone:
    name: str
    polygon: tuple[tuple[float, float], ...]  # (lat, lon) corners, in order

    def contains(self, lat: float, lon: float) -> bool:
        return _point_in_polygon(lat, lon, self.polygon)


def _point_in_polygon(lat: float, lon: float, poly: tuple[tuple[float, float], ...]) -> bool:
    """Ray casting: is ``(lat, lon)`` inside polygon ``poly``?"""
    inside = False
    n = len(poly)
    j = n - 1
    for i in range(n):
        lat_i, lon_i = poly[i]
        lat_j, lon_j = poly[j]
        # Does the edge j->i straddle the horizontal ray at y = lat?
        if (lat_i > lat) != (lat_j > lat):
            # x-coordinate where that edge crosses the ray.
            lon_cross = lon_i + (lat - lat_i) / (lat_j - lat_i) * (lon_j - lon_i)
            if lon < lon_cross:
                inside = not inside
        j = i
    return inside


# A few zones over Bucharest, sized and placed so the fleet actually drives
# through them (the routes span ~44.39-44.47 lat, ~26.04-26.17 lon). Overlaps are
# fine and even useful — a vehicle can be inside two zones at once.
ZONES: tuple[Zone, ...] = (
    Zone(
        "centru",  # central rectangle
        ((44.44, 26.08), (44.44, 26.12), (44.42, 26.12), (44.42, 26.08)),
    ),
    Zone(
        "gara_nord",  # north-west rectangle
        ((44.46, 26.05), (44.46, 26.09), (44.44, 26.09), (44.44, 26.05)),
    ),
    Zone(
        "est",  # eastern quadrilateral (non-rectangular, exercises the ray test)
        ((44.45, 26.13), (44.44, 26.16), (44.42, 26.15), (44.42, 26.13)),
    ),
)


def zones_containing(lat: float, lon: float) -> set[str]:
    """The set of zone names whose polygon contains this point (possibly empty)."""
    return {z.name for z in ZONES if z.contains(lat, lon)}

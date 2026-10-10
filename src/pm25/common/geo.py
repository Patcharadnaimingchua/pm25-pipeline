"""Geo helpers used by DQ rules and (later) mirrored in Spark built-in functions."""

from __future__ import annotations

import math
from dataclasses import dataclass

EARTH_RADIUS_KM = 6371.0


@dataclass(frozen=True)
class BBox:
    west: float
    south: float
    east: float
    north: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.south <= lat <= self.north and self.west <= lon <= self.east

    def as_firms(self) -> str:
        """FIRMS area API expects west,south,east,north."""
        return f"{self.west},{self.south},{self.east},{self.north}"


# Approximate; replace with a real boundary check once the province shapefile is loaded (DQ-08).
THAILAND_BBOX = BBox(west=97.3, south=5.5, east=105.7, north=20.6)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = p2 - p1
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def grid_cell(lat: float, lon: float, size_deg: float = 1.0) -> tuple[int, int]:
    """Cell used by the grid-bucket spatial join (design doc section 17)."""
    return math.floor(lat / size_deg), math.floor(lon / size_deg)

"""Open-Meteo Historical Weather API (ERA5 reanalysis).

Verified 2026-10-02 (open-meteo.com): no API key, non-commercial free up to 10,000 calls/day,
CC BY 4.0, ERA5 from 1940. Variable names below follow the Open-Meteo convention as we know it —
VERIFY each name (especially boundary_layer_height) with one real call in Week 1.
ERA5 is reanalysis on a grid, not a station measurement: say so on every chart that uses it.
"""

from __future__ import annotations

from datetime import date

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "boundary_layer_height",
)


def archive_params(lat: float, lon: float, start: date, end: date,
                   variables: tuple[str, ...] = HOURLY_VARIABLES) -> dict:
    if end < start:
        raise ValueError("end before start")
    return {
        "latitude": round(lat, 4),
        "longitude": round(lon, 4),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(variables),
        "timezone": "UTC",  # always UTC in the lake
    }

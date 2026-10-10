"""NASA FIRMS active fire (VIIRS) — area API, CSV only.

Verified 2026-10-02 (https://firms.modaps.eosdis.nasa.gov/api/area/):
  /api/area/csv/[MAP_KEY]/[SOURCE]/[west,south,east,north]/[DAY_RANGE]/[DATE]
  DAY_RANGE 1..5; DATE = first day, returns DATE .. DATE + DAY_RANGE - 1
  limit: 5,000 transactions / 10 minutes per MAP_KEY; multi-day requests count as several
"""

from __future__ import annotations

import csv
import io
from datetime import date, timedelta

from pm25.common.geo import BBox

BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

SOURCES = {
    "VIIRS_SNPP_SP", "VIIRS_SNPP_NRT",
    "VIIRS_NOAA20_SP", "VIIRS_NOAA20_NRT",
    "VIIRS_NOAA21_NRT",
    "MODIS_SP", "MODIS_NRT",
}

# From the FIRMS VIIRS attribute table as we remember it. VERIFY against a real header in Week 1;
# parse_csv() only requires the columns the pipeline actually uses.
REQUIRED_COLUMNS = {"latitude", "longitude", "acq_date", "acq_time", "confidence", "frp"}


class FirmsPayloadError(ValueError):
    pass


def area_url(map_key: str, source: str, bbox: BBox, day_range: int, start: date | None = None) -> str:
    if not map_key:
        raise ValueError("FIRMS_MAP_KEY is empty")
    if source not in SOURCES:
        raise ValueError(f"unknown FIRMS source {source!r}")
    if not 1 <= day_range <= 5:
        raise ValueError("FIRMS day_range must be 1..5")
    url = f"{BASE_URL}/{map_key}/{source}/{bbox.as_firms()}/{day_range}"
    return f"{url}/{start.isoformat()}" if start else url


def date_chunks(start: date, end: date, max_days: int = 5):
    """Split [start, end] (inclusive) into (chunk_start, n_days) with n_days <= max_days."""
    if end < start:
        raise ValueError("end before start")
    cur = start
    while cur <= end:
        n = min(max_days, (end - cur).days + 1)
        yield cur, n
        cur += timedelta(days=n)


def parse_csv(body: bytes) -> list[dict]:
    text = body.decode("utf-8", errors="replace").strip()
    # FIRMS answers errors such as "Invalid MAP_KEY." as plain text with no CSV header.
    if not text or "latitude" not in text.splitlines()[0]:
        raise FirmsPayloadError(f"not a FIRMS CSV: {text[:120]!r}")
    reader = csv.DictReader(io.StringIO(text))
    missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
    if missing:
        raise FirmsPayloadError(f"missing columns {sorted(missing)}; header={reader.fieldnames}")
    return list(reader)


def detection_key(row: dict) -> tuple:
    """Business key for dedup (NRT vs SP of the same detection, repeated downloads)."""
    return (
        row.get("satellite"), round(float(row["latitude"]), 5), round(float(row["longitude"]), 5),
        row["acq_date"], str(row["acq_time"]).zfill(4),
    )

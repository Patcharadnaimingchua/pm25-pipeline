"""OpenAQ — Air4Thai station data via the public S3 archive (bulk) and API v3 (location list).

Verified 2026-10-02:
  S3 bucket openaq-data-archive (us-east-1), anonymous access, key layout
    records/csv.gz/locationid={id}/year={yyyy}/month={mm}/location-{id}-{yyyymmdd}.csv.gz
  location 368400 (provider Air4Thai): year folders 2022..2025; March 2024 has 21 of 31 daily files
  API v3 GET /v3/locations accepts iso, countries_id, providers_id, limit, page; header X-API-Key
  Free API limits: 60 requests/minute, 2,000/hour
"""

from __future__ import annotations

from datetime import date

API_BASE = "https://api.openaq.org/v3"
BUCKET = "openaq-data-archive"
REGION = "us-east-1"

ARCHIVE_COLUMNS = ("location_id", "sensors_id", "location", "datetime", "lat", "lon",
                   "parameter", "units", "value")


def daily_key(location_id: int, day: date) -> str:
    return (
        f"records/csv.gz/locationid={int(location_id)}/year={day.year}/month={day.month:02d}/"
        f"location-{int(location_id)}-{day.strftime('%Y%m%d')}.csv.gz"
    )


def month_prefix(location_id: int, year: int, month: int) -> str:
    return f"records/csv.gz/locationid={int(location_id)}/year={year}/month={month:02d}/"


def location_prefix(location_id: int) -> str:
    return f"records/csv.gz/locationid={int(location_id)}/"


def anonymous_s3_client(max_pool_connections: int = 64):
    """Unsigned (no AWS account) client. botocore's default pool is 10 connections, which silently
    caps parallel downloads at 10 no matter how many threads run — raise it to match the policy."""
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    return boto3.client("s3", region_name=REGION, config=Config(
        signature_version=UNSIGNED, max_pool_connections=max_pool_connections,
        retries={"max_attempts": 5, "mode": "adaptive"},  # adaptive = slows itself down on S3 throttling
    ))


def locations_url() -> str:
    return f"{API_BASE}/locations"


def summarize_location(loc: dict) -> dict:
    """Flatten one /v3/locations result into a seed-file row."""
    provider = (loc.get("provider") or {}).get("name")
    coords = loc.get("coordinates") or {}
    first = (loc.get("datetimeFirst") or {}).get("utc")
    last = (loc.get("datetimeLast") or {}).get("utc")
    params = sorted({(s.get("parameter") or {}).get("name") for s in loc.get("sensors") or []} - {None})
    return {
        "location_id": loc.get("id"),
        "name": loc.get("name"),
        "provider": provider,
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "is_mobile": loc.get("isMobile"),
        "is_monitor": loc.get("isMonitor"),
        "datetime_first_utc": first,
        "datetime_last_utc": last,
        "parameters": "|".join(params),
    }

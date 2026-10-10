"""Daily incremental landing for OpenAQ, FIRMS and Open-Meteo into the lake raw zone.

Each function lands the files for ONE Airflow data interval, under a raw prefix unique to the
run and try (write-once), plus a manifest. Bulk history is loaded by scripts/backfill/run.py;
these keep the lake current day by day with the same layout.

All external calls go through the polite client (HTTP) or the paced anonymous S3 client.
"""

from __future__ import annotations

import csv
import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

from botocore.exceptions import ClientError

from pm25.backfill.sources import Pacer, Point, grid_points
from pm25.common.geo import BBox
from pm25.common.timeutil import date_range, utc_now
from pm25.ingestion import firms, openaq, openmeteo
from pm25.ingestion.http import HttpResult
from pm25.lake import paths
from pm25.lake.manifest import FetchRecord, Manifest, iso, sha256_hex
from pm25.lake.s3 import put_raw

log = logging.getLogger(__name__)

SEED_CANDIDATES = (Path("/opt/airflow/seeds"), Path(__file__).resolve().parents[3] / "seeds")


class NothingLanded(RuntimeError):
    """Every request failed — fail the task so Airflow retries."""


@dataclass
class Landing:
    """Collects files for one run and writes them + the manifest to raw/ (write-once)."""

    lake: object
    bucket: str
    source: str
    dataset: str
    run_key: str
    interval_start: datetime
    interval_end: datetime
    manifest: Manifest = field(init=False)

    def __post_init__(self):
        self.ingest_date = utc_now().date()
        self.manifest = Manifest(self.source, self.dataset, self.run_key, iso(self.interval_start),
                                 iso(self.interval_end), iso(utc_now()))

    def add(self, name: str, url: str, body: bytes | None, status: int, records: int | None = None,
            error: str | None = None, content_type: str = "application/octet-stream") -> None:
        key = paths.raw_key(self.source, self.dataset, self.ingest_date, self.run_key, name)
        if body is not None:
            put_raw(self.lake, self.bucket, key, body, content_type)
        self.manifest.add(FetchRecord(
            url=url, object_key=key if body is not None else "", http_status=status,
            bytes=len(body or b""), sha256=sha256_hex(body) if body is not None else "",
            fetched_at=iso(utc_now()), records=records, error=error,
        ))

    def finish(self, require_some: bool = True) -> dict:
        mkey = paths.manifest_key(self.source, self.dataset, self.ingest_date, self.run_key)
        put_raw(self.lake, self.bucket, mkey, self.manifest.to_json(), "application/json")
        summary = json.loads(self.manifest.to_json())["summary"]
        summary.update(manifest_key=mkey,
                       raw_prefix=paths.raw_prefix(self.source, self.dataset, self.ingest_date, self.run_key))
        if require_some and summary["files"] and not summary["ok"]:
            raise NothingLanded(f"{self.source}/{self.dataset}: all {summary['files']} requests failed")
        return summary


def _seed(name: str) -> Path:
    for folder in SEED_CANDIDATES:
        if (folder / name).exists():
            return folder / name
    raise FileNotFoundError(f"seeds/{name} not found — run the Week-1 scripts first")


def load_locations(provider: str = "Air4Thai") -> list[dict]:
    with _seed("openaq_th_locations.csv").open(encoding="utf-8") as fh:
        return [r for r in csv.DictReader(fh) if provider.lower() in (r["provider"] or "").lower()
                and r["latitude"] and r["longitude"]]


# ------------------------------------------------------------------ OpenAQ


def land_openaq_days(*, lake, bucket: str, run_key: str, interval_start: datetime, interval_end: datetime,
                     location_ids: list[int], lookback_days: int = 2, s3=None,
                     rate_per_sec: float = 120.0) -> dict:
    """Daily files of each location for the interval day and `lookback_days` before it
    (the archive patches recent files retroactively). Missing file = station had no data: not an error."""
    s3 = s3 or openaq.anonymous_s3_client()
    pacer = Pacer(rate_per_sec)
    day = interval_start.date()
    days = list(date_range(day - timedelta(days=lookback_days), day))
    landing = Landing(lake, bucket, "openaq", "measurements", run_key, interval_start, interval_end)
    missing = 0
    for loc in location_ids:
        for d in days:
            key = openaq.daily_key(loc, d)
            pacer.wait()
            try:
                body = s3.get_object(Bucket=openaq.BUCKET, Key=key)["Body"].read()
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                    missing += 1
                    continue
                landing.add(f"locationid={loc}__{key.rsplit('/', 1)[-1]}", f"s3://{openaq.BUCKET}/{key}",
                            None, 0, error=f"ClientError: {exc}")
                continue
            landing.add(f"locationid={loc}__{key.rsplit('/', 1)[-1]}", f"s3://{openaq.BUCKET}/{key}", body,
                        200, content_type="application/gzip")
    summary = landing.finish(require_some=False)  # quiet days with no files are possible
    summary["missing_files"] = missing
    return summary


# ------------------------------------------------------------------ FIRMS


def land_firms_days(*, lake, bucket: str, run_key: str, interval_start: datetime, interval_end: datetime,
                    fetch: Callable[..., HttpResult], map_key: str, sources: list[str], bbox: BBox,
                    lookback_days: int = 4) -> dict:
    """NRT detections for the interval day and the days before it (FIRMS max range is 5 days).
    Overlap across runs is expected; staging deduplicates by detection key."""
    day = interval_start.date()
    start = day - timedelta(days=lookback_days)
    n = (day - start).days + 1
    landing = Landing(lake, bucket, "firms", "viirs_area_csv", run_key, interval_start, interval_end)
    for source in sources:
        url = firms.area_url(map_key, source, bbox, n, start)
        name = f"{source}__{start.isoformat()}_{n}d.csv"
        try:
            res = fetch(url, weight=n, timeout=120)
            rows = firms.parse_csv(res.body)
            landing.add(name, url, res.body, res.status, records=len(rows), content_type="text/csv")
        except firms.FirmsPayloadError as exc:  # e.g. "Invalid MAP_KEY." — fail loudly, do not retry silently
            raise RuntimeError(f"FIRMS {source}: {exc}") from exc
        except Exception as exc:
            landing.add(name, url, None, 0, error=f"{type(exc).__name__}: {exc}")
    return landing.finish()


# ------------------------------------------------------------------ Open-Meteo (ERA5)


def weather_points() -> tuple[list[Point], dict[str, str]]:
    return grid_points(load_locations())


def land_weather_days(*, lake, bucket: str, run_key: str, interval_start: datetime, interval_end: datetime,
                      fetch: Callable[..., HttpResult], points: list[Point], lag_days: int = 6,
                      window_days: int = 3) -> dict:
    """ERA5 reaches the archive with a delay of several days, so each run fetches a small window
    ending `lag_days` before the interval day. VERIFY the actual delay in Week 1 and tune lag_days."""
    end = interval_start.date() - timedelta(days=lag_days)
    start = end - timedelta(days=window_days - 1)
    landing = Landing(lake, bucket, "openmeteo", "era5_hourly", run_key, interval_start, interval_end)
    for p in points:
        params = openmeteo.archive_params(p.lat, p.lon, start, end)
        name = f"{p.point_id}__{start.isoformat()}_{end.isoformat()}.json"
        try:
            res = fetch(openmeteo.ARCHIVE_URL, params=params, weight=window_days, timeout=120)
            payload = json.loads(res.body)
            if "hourly" not in payload:
                raise ValueError(f"no 'hourly' in response: {res.body[:120]!r}")
            landing.add(name, f"{openmeteo.ARCHIVE_URL}?{p.point_id}", res.body, res.status,
                        records=len(payload["hourly"].get("time", [])), content_type="application/json")
        except Exception as exc:
            landing.add(name, f"{openmeteo.ARCHIVE_URL}?{p.point_id}", None, 0, error=f"{type(exc).__name__}: {exc}")
    return landing.finish()

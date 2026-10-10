"""Land one hour of GISTDA modeled PM2.5 into raw/ (province + every district).

Kept outside the DAG file so it can be unit-tested without Airflow.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from pm25.common.timeutil import utc_now
from pm25.ingestion import gistda
from pm25.ingestion.http import HttpResult, http_get
from pm25.lake import paths
from pm25.lake.manifest import FetchRecord, Manifest, iso, sha256_hex
from pm25.lake.s3 import put_raw

log = logging.getLogger(__name__)

SOURCE = "gistda"
DATASET_HOURLY = "pm25_amphoe_hourly"
DATASET_24H = "pm25_province_24h"
EXPECTED_PROVINCES = 77  # verified 2026-10-02


class TooManyFailures(RuntimeError):
    pass


def _fetch(fetch: Callable[[str], HttpResult], url: str):
    try:
        return fetch(url), None
    except Exception as exc:  # recorded in the manifest, judged afterwards
        return None, f"{type(exc).__name__}: {exc}"


def land_hour(
    *,
    client,
    bucket: str,
    run_id: str,
    try_number: int,
    interval_start: datetime,
    interval_end: datetime,
    fetch: Callable[[str], HttpResult] = http_get,
    max_workers: int = 4,
    max_failed_pct: float = 10.0,
) -> dict:
    """Fetch province list + all districts for the current hour and write them under raw/.

    Each Airflow try writes its own prefix (run_id + try number), so a retry never collides
    with files a failed attempt already landed — raw stays write-once.
    """
    ingest_date = utc_now().date()
    run_key = f"{run_id}__try{try_number}"
    manifest = Manifest(SOURCE, DATASET_HOURLY, run_key, iso(interval_start), iso(interval_end), iso(utc_now()))

    def land(url: str, filename: str, result: HttpResult | None, error: str | None, records: int | None):
        key = paths.raw_key(SOURCE, DATASET_HOURLY, ingest_date, run_key, filename)
        # Land whatever the source answered with HTTP success — even a malformed payload is
        # evidence worth keeping in raw. The manifest records whether it parsed.
        if result is not None:
            put_raw(client, bucket, key, result.body, "application/json")
        manifest.add(FetchRecord(
            url=url, object_key=key if result is not None else "",
            http_status=result.status if result else 0,
            bytes=len(result.body) if result else 0,
            sha256=sha256_hex(result.body) if result else "",
            fetched_at=iso(utc_now()), records=records, error=error,
        ))

    # 1) provinces — without this list nothing else can run, so a failure here fails the task
    prov_url = gistda.province_url()
    result, error = _fetch(fetch, prov_url)
    if error:
        raise RuntimeError(f"province list failed: {error}")
    provinces = gistda.parse_province(result.body)
    land(prov_url, "province.json", result, None, len(provinces))
    dt_warning = gistda.check_dt_plausible(max(r["event_ts"] for r in provinces), utc_now())
    if dt_warning:
        log.warning("GISTDA timestamp check: %s", dt_warning)
        manifest.notes.append(f"DQ-14: {dt_warning}")
    codes = gistda.province_codes(provinces)
    if len(codes) != EXPECTED_PROVINCES:
        log.warning("expected %d provinces, got %d", EXPECTED_PROVINCES, len(codes))

    # 2) districts, a few requests at a time (be polite to a public service)
    urls = {code: gistda.amphoe_url(code) for code in codes}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = dict(zip(urls, pool.map(lambda u: _fetch(fetch, u), urls.values()), strict=True))

    failed = []
    for code, (res, err) in results.items():
        n = None
        if err is None:
            try:
                n = len(gistda.parse_amphoe(res.body, code))
            except gistda.GistdaPayloadError as exc:
                err = f"payload: {exc}"
        if err:
            failed.append(code)
        land(urls[code], f"amphoe_pv_idn={code}.json", res, err, n)

    manifest_key = paths.manifest_key(SOURCE, DATASET_HOURLY, ingest_date, run_key)
    put_raw(client, bucket, manifest_key, manifest.to_json(), "application/json")

    summary = json.loads(manifest.to_json())["summary"]
    summary.update(raw_prefix=paths.raw_prefix(SOURCE, DATASET_HOURLY, ingest_date, run_key),
                   manifest_key=manifest_key, failed_provinces=failed, provinces=len(codes))
    failed_pct = 100.0 * len(failed) / max(len(codes), 1)
    if failed_pct > max_failed_pct:
        raise TooManyFailures(f"{len(failed)}/{len(codes)} provinces failed ({failed_pct:.0f}%)")
    return summary


def land_province_24h(
    *,
    client,
    bucket: str,
    run_id: str,
    try_number: int,
    interval_start: datetime,
    interval_end: datetime,
    province_codes: list[int],
    fetch: Callable[[str], HttpResult] = http_get,
    max_workers: int = 4,
) -> dict:
    """Daily safety net: the last 24 hourly values of every province. If the hourly DAG
    missed hours (laptop asleep, API down), province-level values can still be recovered."""
    ingest_date = utc_now().date()
    run_key = f"{run_id}__try{try_number}"
    manifest = Manifest(SOURCE, DATASET_24H, run_key, iso(interval_start), iso(interval_end), iso(utc_now()))
    urls = {code: gistda.province_24h_url(code) for code in province_codes}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        results = dict(zip(urls, pool.map(lambda u: _fetch(fetch, u), urls.values()), strict=True))
    for code, (res, err) in results.items():
        key = paths.raw_key(SOURCE, DATASET_24H, ingest_date, run_key, f"province_24h_pv_idn={code}.json")
        n = None
        if err is None:
            put_raw(client, bucket, key, res.body, "application/json")
            try:
                n = len(gistda.parse_24h(res.body, "province", code))
            except gistda.GistdaPayloadError as exc:
                err = f"payload: {exc}"
        manifest.add(FetchRecord(
            url=urls[code], object_key=key if res is not None else "",
            http_status=res.status if res else 0, bytes=len(res.body) if res else 0,
            sha256=sha256_hex(res.body) if res else "", fetched_at=iso(utc_now()), records=n, error=err,
        ))
    manifest_key = paths.manifest_key(SOURCE, DATASET_24H, ingest_date, run_key)
    put_raw(client, bucket, manifest_key, manifest.to_json(), "application/json")
    summary = json.loads(manifest.to_json())["summary"]
    summary.update(manifest_key=manifest_key)
    return summary


def check_manifest(manifest: dict, expected_provinces: int = EXPECTED_PROVINCES,
                   max_failed_pct: float = 10.0) -> list[tuple[str, str, str]]:
    """DQ on raw (design doc DQ-01, DQ-18 style). Returns (rule_id, severity, message) tuples."""
    issues = []
    files = manifest.get("files", [])
    ok = [f for f in files if not f.get("error")]
    failed = [f for f in files if f.get("error")]
    if not ok:
        issues.append(("DQ-01", "FAIL_RUN", "no file landed"))
    if files and 100.0 * len(failed) / len(files) > max_failed_pct:
        issues.append(("DQ-01", "FAIL_RUN", f"{len(failed)} of {len(files)} requests failed"))
    province = next((f for f in ok if f["object_key"].endswith("/province.json")), None)
    if province and province.get("records") != expected_provinces:
        issues.append(("DQ-18", "WARN", f"province records {province.get('records')} != {expected_provinces}"))
    if any((f.get("records") or 0) == 0 for f in ok):
        issues.append(("DQ-01", "WARN", "some files contain zero records"))
    for note in manifest.get("notes", []):
        if note.startswith("DQ-14"):
            issues.append(("DQ-14", "WARN", note))
    return issues

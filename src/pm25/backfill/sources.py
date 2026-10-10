"""Bulk historical downloads into a local landing folder (data/landing/<source>/...).

Every function is resumable (files already on disk are skipped), paced by the source policy and
logged in the request ledger. `push_to_lake` later copies landed files into the lake raw zone with
a manifest, so lineage is the same as for scheduled DAG runs.
"""

from __future__ import annotations

import csv
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from pm25.common.geo import BBox
from pm25.ingestion import firms, openaq, openmeteo
from pm25.ingestion.http import RetryableHttpError
from pm25.ingestion.polite import CircuitOpen, HostPolicy, Ledger, PoliteClient

log = logging.getLogger(__name__)


# ------------------------------------------------------------------ OpenAQ (S3 open data)


def _file_date(key: str) -> date:
    stamp = key.rsplit("-", 1)[-1].split(".")[0]
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))


class Pacer:
    """Thread-safe pacing: request starts are at least 1/rate seconds apart across all threads."""

    def __init__(self, rate_per_sec: float, clock=time.monotonic, sleep=time.sleep):
        self.interval = 1.0 / rate_per_sec
        self.clock, self.sleep = clock, sleep
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self.clock()
            start = max(now, self._next)
            self._next = start + self.interval
        if start > now:
            self.sleep(start - now)


def openaq_archive(location_ids: list[int], start: date, end: date, dest: Path,
                   policy: HostPolicy, ledger: Ledger, s3=None, progress_every: int = 2000) -> dict:
    """Mirror the OpenAQ archive files of the given locations for [start, end].

    One listing per location-year (a year holds <= 366 daily files, i.e. one page), done in parallel,
    then parallel downloads — all through one shared pacer capped by the source policy.
    """
    s3 = s3 or openaq.anonymous_s3_client()
    pacer = Pacer(policy.rate_per_sec)
    workers = max(1, policy.max_concurrency)
    s3 = s3 or openaq.anonymous_s3_client(max_pool_connections=workers + 8)

    def list_year(task):
        loc, year = task
        prefix = f"{openaq.location_prefix(loc)}year={year}/"
        found = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=openaq.BUCKET, Prefix=prefix):
            pacer.wait()  # paginate() fetches lazily: one page per iteration
            ledger.write(event="request", host=policy.host, url=f"s3://{openaq.BUCKET}/{prefix}",
                         status=200, weight=1, kind="list")
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv.gz") and start <= _file_date(obj["Key"]) <= end:
                    found.append((loc, obj["Key"], obj["Size"]))
        return found

    tasks = [(loc, y) for loc in location_ids for y in range(start.year, end.year + 1)]
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        listed = [item for chunk in pool.map(list_year, tasks) for item in chunk]
    jobs = []
    for loc, key, size in listed:
        target = dest / f"locationid={loc}" / key.rsplit("/", 1)[-1]
        if not target.exists():
            jobs.append((key, target, size))
    print(f"listed {len(tasks):,} location-years in {time.monotonic() - t0:.0f}s: "
          f"{len(listed):,} files in window, {len(jobs):,} to download", flush=True)

    done = {"n": 0, "bytes": 0}
    lock = threading.Lock()
    t1 = time.monotonic()

    def fetch(job):
        key, target, size = job
        pacer.wait()
        body = s3.get_object(Bucket=openaq.BUCKET, Key=key)["Body"].read()
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".part")
        tmp.write_bytes(body)
        tmp.rename(target)  # atomic: a crash never leaves a half file that looks complete
        ledger.write(event="request", host=policy.host, url=f"s3://{openaq.BUCKET}/{key}", status=200,
                     bytes=len(body), weight=1, kind="get")
        with lock:
            done["n"] += 1
            done["bytes"] += len(body)
            n = done["n"]
        if n % progress_every == 0 or n == len(jobs):
            rate = n / max(time.monotonic() - t1, 1e-9)
            eta = (len(jobs) - n) / rate if rate else 0
            print(f"  downloaded {n:,}/{len(jobs):,} ({rate:.0f} files/s, ~{eta / 60:.0f} min left)", flush=True)
        return len(body)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(fetch, jobs))
    return {"files_in_window": len(listed), "downloaded": len(jobs),
            "downloaded_mb": round(done["bytes"] / 1e6, 2)}


# ------------------------------------------------------------------ FIRMS


def firms_archive(client: PoliteClient, map_key: str, sources: list[str], bbox: BBox, start: date, end: date,
                  dest: Path) -> dict:
    """One CSV per (source, 5-day chunk). Each request is weighted by its day range (FIRMS rule)."""
    out = {}
    for source in sources:
        folder = dest / source
        folder.mkdir(parents=True, exist_ok=True)
        new = 0
        for chunk_start, n in firms.date_chunks(start, end):
            target = folder / f"{chunk_start.isoformat()}_{n}d.csv"
            if target.exists():
                continue
            body = client.get(firms.area_url(map_key, source, bbox, n, chunk_start), weight=n, timeout=120).body
            firms.parse_csv(body)  # stop at once on "Invalid MAP_KEY." instead of saving error text
            target.write_bytes(body)
            new += 1
        out[source] = new
    return out


# ------------------------------------------------------------------ Open-Meteo (ERA5)


@dataclass(frozen=True)
class Point:
    point_id: str
    lat: float
    lon: float


def grid_points(stations: list[dict], cell_deg: float = 0.1) -> tuple[list[Point], dict[str, str]]:
    """Collapse stations that fall in the same small cell into one request point.

    ERA5 is a grid: two Bangkok stations 3 km apart get identical values. Fetching both would put
    duplicate rows in the lake and inflate the record count — so we fetch once per cell and keep a
    station -> point mapping for the join.
    """
    points: dict[tuple[int, int], Point] = {}
    mapping: dict[str, str] = {}
    for s in stations:
        lat, lon = float(s["latitude"]), float(s["longitude"])
        cell = (round(lat / cell_deg), round(lon / cell_deg))
        if cell not in points:
            points[cell] = Point(f"cell_{cell[0]}_{cell[1]}", round(cell[0] * cell_deg, 4),
                                 round(cell[1] * cell_deg, 4))
        mapping[str(s["location_id"])] = points[cell].point_id
    return list(points.values()), mapping


def openmeteo_archive(client: PoliteClient, points: list[Point], start: date, end: date, dest: Path) -> dict:
    """One request per point per calendar year (small responses, easy to resume).

    A failed request is logged and skipped — the run continues and the summary lists what is left,
    so re-running the same command fetches only the missing files.
    """
    dest.mkdir(parents=True, exist_ok=True)
    new, failed = 0, []
    for p in points:
        for year in range(start.year, end.year + 1):
            s, e = max(start, date(year, 1, 1)), min(end, date(year, 12, 31))
            target = dest / f"{p.point_id}_{year}.json"
            if target.exists():
                continue
            try:
                res = client.get(openmeteo.ARCHIVE_URL, params=openmeteo.archive_params(p.lat, p.lon, s, e),
                                 weight=(e - s).days + 1, timeout=120)
                payload = json.loads(res.body)
                if "hourly" not in payload:
                    raise ValueError(f"no 'hourly' in response: {res.body[:120]!r}")
            except CircuitOpen:
                # the host refused several requests in a row: quota used up. Stop politely instead of
                # skipping through every remaining file.
                resume = (datetime.now() + timedelta(hours=1)).strftime("%H:%M")
                print(f"  Open-Meteo keeps answering 429 — quota for now is used up. Stopped. "
                      f"Re-run the same command after {resume} (or tomorrow if it happens again).", flush=True)
                return {"points": len(points), "new_files": new, "failed": len(failed) + 1,
                        "stopped_early": True, "rerun_after": resume}
            except (RetryableHttpError, ValueError) as exc:  # JSONDecodeError is a ValueError
                failed.append(f"{p.point_id}_{year}")
                log.warning("skip %s %s: %s", p.point_id, year, exc)
                print(f"  skip {p.point_id} {year}: {type(exc).__name__}", flush=True)
                continue
            target.write_bytes(res.body)
            new += 1
            if new % 25 == 0:
                print(f"  weather: {new} new files", flush=True)
    return {"points": len(points), "new_files": new, "failed": len(failed),
            "rerun_to_fetch": failed[:10] + (["..."] if len(failed) > 10 else [])}


def write_mapping(mapping: dict[str, str], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["location_id", "point_id"])
        w.writerows(sorted(mapping.items()))

"""Bulk backfill, the polite way. Run on a laptop or server with internet.

    python scripts/backfill/run.py openaq    --start 2022-01-01 --end 2025-12-31 --provider Air4Thai
    python scripts/backfill/run.py firms     --start 2022-01-01 --end 2025-12-31 --sources VIIRS_SNPP_SP VIIRS_NOAA20_SP
    python scripts/backfill/run.py weather   --start 2022-01-01 --end 2025-12-31 --provider Air4Thai
    python scripts/backfill/run.py push      --source openaq     (needs the Docker stack: MinIO on localhost:9000)
    python scripts/backfill/run.py report    (requests per host vs published limits)

Everything is resumable: stop with Ctrl+C and run the same command again.
Rules each source is held to: config/source_policies.yaml. Why: docs/data_ethics.md.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts" / "week1"))

from _common import append_report, load_dotenv  # noqa: E402

load_dotenv()

from pm25.backfill import push, sources  # noqa: E402
from pm25.common.geo import THAILAND_BBOX, BBox  # noqa: E402
from pm25.ingestion.polite import Ledger, default_client, load_policies  # noqa: E402
from pm25.settings import get_settings, load_sources_config  # noqa: E402

LANDING = ROOT / "data" / "landing"
LEDGER = ROOT / "data" / "ledger"
SEED = ROOT / "seeds" / "openaq_th_locations.csv"


def stations(provider: str) -> list[dict]:
    if not SEED.exists():
        sys.exit("seeds/openaq_th_locations.csv missing: run `make week1-openaq-locations` first")
    with SEED.open(encoding="utf-8") as fh:
        rows = [r for r in csv.DictReader(fh) if provider.lower() in (r["provider"] or "").lower()]
    return [r for r in rows if r["latitude"] and r["longitude"]]


def cmd_openaq(a) -> None:
    policies, _ = load_policies()
    policy = policies["openaq-data-archive.s3.amazonaws.com"]
    ids = [int(r["location_id"]) for r in stations(a.provider)]
    print(f"{len(ids)} locations (provider ~ {a.provider!r})")
    out = sources.openaq_archive(ids, a.start, a.end, LANDING / "openaq", policy, Ledger(LEDGER))
    print(out)


def cmd_firms(a) -> None:
    key = get_settings().firms_map_key or sys.exit("FIRMS_MAP_KEY is not set (.env)")
    if a.regional:
        bbox = BBox(**load_sources_config()["sources"]["firms"]["regional_bbox"])
    else:
        bbox = THAILAND_BBOX
    out = sources.firms_archive(default_client(LEDGER), key, a.sources, bbox, a.start, a.end,
                                LANDING / ("firms_regional" if a.regional else "firms"))
    print(out)


def cmd_weather(a) -> None:
    pts, mapping = sources.grid_points(stations(a.provider), cell_deg=a.cell_deg)
    sources.write_mapping(mapping, ROOT / "seeds" / "station_weather_point.csv")
    print(f"{len(mapping)} stations -> {len(pts)} distinct weather points (cell {a.cell_deg} deg)")
    # breaker after 3 consecutive give-ups; fewer retries per file so a quota wall is detected quickly
    client = default_client(LEDGER, max_retries=3, breaker_threshold=3, breaker_cooldown_s=3600)
    out = sources.openmeteo_archive(client, pts, a.start, a.end, LANDING / "openmeteo")
    print(out)


PUSH = {  # landing folder -> (source, dataset, glob, content type)
    "openaq": ("openaq", "measurements", "**/*.csv.gz", "application/gzip"),
    "firms": ("firms", "viirs_area_csv", "**/*.csv", "text/csv"),
    "firms_regional": ("firms", "viirs_area_csv_regional", "**/*.csv", "text/csv"),
    "openmeteo": ("openmeteo", "era5_hourly", "*.json", "application/json"),
}


def cmd_push(a) -> None:
    from pm25.lake.s3 import lake_client

    os.environ.setdefault("S3_ENDPOINT_URL", "http://localhost:9000")
    if os.environ["S3_ENDPOINT_URL"].startswith("http://minio:"):
        os.environ["S3_ENDPOINT_URL"] = "http://localhost:9000"  # running outside Docker
    get_settings.cache_clear()
    source, dataset, pattern, ctype = PUSH[a.source]
    run_id = "backfill__" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = push.push_landing(source=source, dataset=dataset, landing_dir=LANDING / a.source, pattern=pattern,
                            s3=lake_client(), bucket=get_settings().lake_bucket, run_id=run_id,
                            content_type=ctype)
    print(out)


def cmd_report(_a) -> None:
    rows = push.ledger_summary(LEDGER)
    if not rows:
        print("ledger is empty")
        return
    cols = list(rows[0])
    table = "| " + " | ".join(cols) + " |\n| " + " | ".join("---" for _ in cols) + " |\n"
    table += "\n".join("| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows)
    print(table)
    append_report("Request ledger vs published limits", table + "\n\nPublished limits: config/source_policies.yaml")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def window(p):
        p.add_argument("--start", type=date.fromisoformat, default=date(2022, 1, 1))
        p.add_argument("--end", type=date.fromisoformat, default=date(2025, 12, 31))

    p = sub.add_parser("openaq")
    window(p)
    p.add_argument("--provider", default="Air4Thai", help="substring; '' = every provider (low-cost too)")
    p.set_defaults(fn=cmd_openaq)
    p = sub.add_parser("firms")
    window(p)
    p.add_argument("--sources", nargs="+", default=["VIIRS_SNPP_SP"])
    p.add_argument("--regional", action="store_true", help="Thailand + neighbours bbox (question Q7)")
    p.set_defaults(fn=cmd_firms)
    p = sub.add_parser("weather")
    window(p)
    p.add_argument("--provider", default="Air4Thai")
    p.add_argument("--cell-deg", type=float, default=0.1)
    p.set_defaults(fn=cmd_weather)
    p = sub.add_parser("push")
    p.add_argument("--source", choices=sorted(PUSH), required=True)
    p.set_defaults(fn=cmd_push)
    p = sub.add_parser("report")
    p.set_defaults(fn=cmd_report)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()

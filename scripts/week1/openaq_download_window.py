"""Gate G1 step 2: download the OpenAQ archive files for the analysis window.

Same code path as the full backfill (scripts/backfill/run.py openaq): files land in
data/landing/openaq/ once and are reused — nothing is downloaded twice.

    python scripts/week1/openaq_download_window.py --start 2022-01-01 --end 2025-12-31 --provider Air4Thai
"""

from __future__ import annotations

import argparse
import csv
from datetime import date

from _common import ROOT, append_report, load_dotenv

load_dotenv()

from pm25.backfill.sources import openaq_archive
from pm25.ingestion.polite import Ledger, load_policies

SEED = ROOT / "seeds" / "openaq_th_locations.csv"
OUT = ROOT / "data" / "landing" / "openaq"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat, required=True)
    ap.add_argument("--end", type=date.fromisoformat, required=True)
    ap.add_argument("--provider", default="Air4Thai", help="substring match, case-insensitive; '' = all")
    args = ap.parse_args()

    with SEED.open(encoding="utf-8") as fh:
        ids = [int(r["location_id"]) for r in csv.DictReader(fh)
               if args.provider.lower() in (r["provider"] or "").lower()]
    print(f"{len(ids)} locations match provider {args.provider!r}")
    policy = load_policies()[0]["openaq-data-archive.s3.amazonaws.com"]
    out = openaq_archive(ids, args.start, args.end, OUT, policy, Ledger(ROOT / "data" / "ledger"))
    print(out)
    with_data = sum(1 for d in OUT.glob("locationid=*") if any(d.iterdir()))
    append_report(
        f"OpenAQ archive files {args.start}..{args.end} (provider ~ {args.provider!r})",
        f"""| Metric | Value |
| --- | --- |
| Locations matching provider | {len(ids)} |
| Locations with files on disk | {with_data} |
| Daily files in window (listed) | {out['files_in_window']:,} |
| Newly downloaded this run | {out['downloaded']:,} ({out['downloaded_mb']} MB) |
""",
    )


if __name__ == "__main__":
    main()

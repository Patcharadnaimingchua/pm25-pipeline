"""Gate G1 step 4: download FIRMS VIIRS detections for Thailand and count real rows.

    python scripts/week1/firms_count.py --start 2022-01-01 --end 2025-12-31 --source VIIRS_SNPP_SP

Uses the polite client (each 5-day request counts as 5 transactions against a self-imposed budget of
4,000 per 10 minutes; FIRMS publishes 5,000). Files land in data/landing/firms/ and are reused by the
backfill. Detections are deduplicated before counting. If SP data is not yet available for the most
recent months, run again with --source VIIRS_SNPP_NRT for that tail.
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date

from _common import ROOT, append_report, load_dotenv

load_dotenv()

from pm25.backfill.sources import firms_archive
from pm25.common.geo import THAILAND_BBOX
from pm25.ingestion import firms
from pm25.ingestion.polite import default_client
from pm25.settings import get_settings

OUT = ROOT / "data" / "landing" / "firms"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat, required=True)
    ap.add_argument("--end", type=date.fromisoformat, required=True)
    ap.add_argument("--source", default="VIIRS_SNPP_SP")
    args = ap.parse_args()
    key = get_settings().firms_map_key
    if not key:
        sys.exit("FIRMS_MAP_KEY is not set (.env)")

    new = firms_archive(default_client(ROOT / "data" / "ledger"), key, [args.source], THAILAND_BBOX,
                        args.start, args.end, OUT)
    seen, per_year, header = set(), Counter(), None
    for chunk_start, n in firms.date_chunks(args.start, args.end):
        rows = firms.parse_csv((OUT / args.source / f"{chunk_start.isoformat()}_{n}d.csv").read_bytes())
        if rows and header is None:
            header = list(rows[0].keys())
        for row in rows:
            k = firms.detection_key(row)
            if k not in seen:
                seen.add(k)
                per_year[row["acq_date"][:4]] += 1

    total = sum(per_year.values())
    print(f"{args.source}: {total:,} unique detections ({new[args.source]} new requests)")
    table = "\n".join(f"| {y} | {n:,} |" for y, n in sorted(per_year.items()))
    append_report(
        f"FIRMS {args.source} Thailand bbox {args.start}..{args.end}",
        f"""Bounding box: {THAILAND_BBOX.as_firms()} (rectangle: includes some border areas of neighbours)

| Year | Unique detections |
| --- | --- |
{table}

Total unique detections: **{total:,}**

Header seen: `{', '.join(header or [])}`
""",
    )


if __name__ == "__main__":
    main()

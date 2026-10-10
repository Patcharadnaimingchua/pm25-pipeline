"""Gate G1 step 1: list OpenAQ locations in Thailand and save them as a seed file.

Needs OPENAQ_API_KEY (free: https://explore.openaq.org/register). Uses /v3/locations?iso=TH,
paging 1,000 at a time and staying far below the free limit (60/min, 2,000/hour).

    python scripts/week1/openaq_th_locations.py
Output: seeds/openaq_th_locations.csv
"""

from __future__ import annotations

import csv
import json
import os
import sys
from collections import Counter

from _common import ROOT, append_report, load_dotenv

load_dotenv()

from pm25.ingestion import openaq
from pm25.ingestion.polite import default_client

OUT = ROOT / "seeds" / "openaq_th_locations.csv"


def main() -> None:
    key = os.environ.get("OPENAQ_API_KEY")
    if not key:
        sys.exit("OPENAQ_API_KEY is not set (.env)")
    client = default_client(ROOT / "data" / "ledger")  # 48/min, 1,600/hour (free tier: 60, 2,000)
    rows, page = [], 1
    while True:
        res = client.get(openaq.locations_url(), params={"iso": "TH", "limit": 1000, "page": page},
                       headers={"X-API-Key": key, "Accept": "application/json"})
        results = json.loads(res.body).get("results", [])
        rows.extend(openaq.summarize_location(loc) for loc in results)
        print(f"page {page}: {len(results)} locations")
        if len(results) < 1000:
            break
        page += 1

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()) if rows else ["location_id"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"saved {len(rows)} locations -> {OUT.relative_to(ROOT)}")

    by_provider = Counter(r["provider"] for r in rows)
    pm25 = Counter(r["provider"] for r in rows if "pm25" in (r["parameters"] or "").split("|"))
    table = "\n".join(f"| {p} | {n} | {pm25.get(p, 0)} |" for p, n in by_provider.most_common())
    append_report(
        "OpenAQ locations in Thailand (API v3, iso=TH)",
        f"| Provider | Locations | Locations with a pm25 sensor |\n| --- | --- | --- |\n{table}\n\n"
        f"Total locations: {len(rows)}",
    )


if __name__ == "__main__":
    main()

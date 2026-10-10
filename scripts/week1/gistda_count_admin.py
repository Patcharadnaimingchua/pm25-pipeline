"""Gate G1 helper: how many provinces and districts does GISTDA actually return right now?

Verifies the 928-district assumption used in the volume estimate (design doc 7.2 / 7.5).
Makes 1 + 77 requests through the polite client.

    python scripts/week1/gistda_count_admin.py
"""

from __future__ import annotations

from _common import ROOT, append_report, load_dotenv

load_dotenv()

from pm25.ingestion import gistda
from pm25.ingestion.polite import default_client


def main() -> None:
    client = default_client(ROOT / "data" / "ledger")  # paced per config/source_policies.yaml
    provinces = gistda.parse_province(client.get(gistda.province_url()).body)
    codes = gistda.province_codes(provinces)
    print(f"provinces: {len(codes)} (dt={provinces[0]['event_ts'].isoformat()})")
    per_province, failed = {}, []
    for code in codes:
        try:
            per_province[code] = len(gistda.parse_amphoe(client.get(gistda.amphoe_url(code)).body, code))
        except Exception as exc:
            failed.append((code, str(exc)))
    total = sum(per_province.values())
    print(f"districts: {total} across {len(per_province)} provinces; failed: {len(failed)}")
    rows_per_day = total * 24
    append_report(
        "GISTDA admin units",
        f"""| Metric | Value |
| --- | --- |
| Provinces returned | {len(codes)} |
| Districts returned (sum over provinces) | {total} |
| Provinces failed | {len(failed)} {failed if failed else ''} |
| District-hour rows per day (= districts x 24) | {rows_per_day:,} |
| Days to collect 1,000,000 district-hour rows | {1_000_000 / rows_per_day:.1f} |
""",
    )


if __name__ == "__main__":
    main()

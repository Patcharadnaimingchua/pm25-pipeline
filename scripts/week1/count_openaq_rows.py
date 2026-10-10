"""Gate G1 step 3: count REAL rows in the downloaded OpenAQ files (after dedup).

    python scripts/week1/count_openaq_rows.py --start 2022-01-01 --end 2025-12-31

Gate G1 passes when PM2.5 rows (deduplicated on sensor + timestamp, inside the window) >= 1,000,000.
DuckDB is only the counting tool here; it is not part of the pipeline.
"""

from __future__ import annotations

import argparse
from datetime import date

from _common import ROOT, append_report

THRESHOLD = 1_000_000
DATA = ROOT / "data" / "landing" / "openaq"


def main() -> None:
    import duckdb

    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat, required=True)
    ap.add_argument("--end", type=date.fromisoformat, required=True)
    args = ap.parse_args()

    pattern = str(DATA / "**" / "*.csv.gz")
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    print("reading files (one pass, no schema union) ...", flush=True)
    # One pass into a table: the old view re-read 174k files twice and union_by_name sniffed every file.
    # Header style differs between files (quoted vs unquoted) -> fixed columns, header rows filtered out.
    cols = ("{'location_id':'VARCHAR','sensors_id':'VARCHAR','location':'VARCHAR','datetime':'VARCHAR',"
            "'lat':'VARCHAR','lon':'VARCHAR','parameter':'VARCHAR','units':'VARCHAR','value':'VARCHAR'}")
    con.execute(f"CREATE TABLE raw AS SELECT sensors_id, location_id, datetime, parameter, value "
                f"FROM read_csv('{pattern}', header = false, columns = {cols}, quote = '\"', escape = '\"', "
                f"delim = ',', strict_mode = false, ignore_errors = true) WHERE sensors_id <> 'sensors_id'")
    raw_rows = con.execute("SELECT count(*) FROM raw").fetchone()[0]
    print(f"raw rows loaded: {raw_rows:,}; deduplicating ...", flush=True)
    con.execute(f"""
        CREATE TABLE clean AS
        SELECT DISTINCT ON (sensors_id, ts)
               location_id, sensors_id, lower(parameter) AS parameter, ts, TRY_CAST(value AS DOUBLE) AS value
        FROM (SELECT *, TRY_CAST(datetime AS TIMESTAMPTZ) AS ts FROM raw)
        WHERE ts IS NOT NULL
          AND ts >= TIMESTAMPTZ '{args.start} 00:00:00+07'
          AND ts <  TIMESTAMPTZ '{args.end} 00:00:00+07' + INTERVAL 1 DAY
        ORDER BY sensors_id, ts
    """)
    by_param = con.execute("""
        SELECT parameter, count(*) AS n, count(DISTINCT location_id) AS stations,
               CAST(min(ts) AS VARCHAR) AS first_ts, CAST(max(ts) AS VARCHAR) AS last_ts,
               count(*) FILTER (WHERE value < 0) AS negative_values
        FROM clean GROUP BY 1 ORDER BY n DESC
    """).fetchall()
    total = sum(r[1] for r in by_param)
    pm25 = next((r[1] for r in by_param if r[0] in ("pm25", "pm2.5")), 0)
    verdict = "PASS" if pm25 >= THRESHOLD else "NOT YET — apply the fallback order in design doc 7.5"

    lines = "\n".join(f"| {p} | {n:,} | {s} | {f} | {lst} | {neg:,} |" for p, n, s, f, lst, neg in by_param)
    print(f"raw rows {raw_rows:,}; deduplicated in window {total:,}; PM2.5 {pm25:,} -> {verdict}")
    append_report(
        f"OpenAQ real row count {args.start}..{args.end}",
        f"""Raw rows read: {raw_rows:,} · after dedup (sensor + timestamp) and window filter: {total:,}

| Parameter | Rows | Stations | First | Last | Negative values |
| --- | --- | --- | --- | --- | --- |
{lines}

**Gate G1 (PM2.5 >= {THRESHOLD:,}): {verdict}** — PM2.5 rows = {pm25:,}
""",
    )


if __name__ == "__main__":
    main()

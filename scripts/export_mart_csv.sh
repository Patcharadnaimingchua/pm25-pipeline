#!/usr/bin/env bash
# Export every mart view/table to CSV (for Power BI "Text/CSV" import). Run from the project root.
set -euo pipefail
OUT="${1:-data/export_csv}"
mkdir -p "$OUT"
PSQL=(docker compose exec -T postgres-dwh psql -U dwh -d pm25 -At)
for name in $("${PSQL[@]}" -c "select table_name from information_schema.tables where table_schema='mart' order by 1"); do
  "${PSQL[@]}" -c "\\copy (select * from mart.${name}) to stdout with (format csv, header true)" > "$OUT/${name}.csv"
  echo "$name: $(($(wc -l < "$OUT/${name}.csv") - 1)) rows"
done
echo "เสร็จ -> $OUT"

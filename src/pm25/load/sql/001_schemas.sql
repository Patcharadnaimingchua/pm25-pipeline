-- Runs automatically on the FIRST start of an empty postgres-dwh volume
-- (docker-entrypoint-initdb.d). To re-run: `docker compose down -v` (deletes DWH data!) or run by hand.

CREATE SCHEMA IF NOT EXISTS stg;    -- per-interval landing of curated Parquet
CREATE SCHEMA IF NOT EXISTS dw;     -- star schema (facts + dimensions)
CREATE SCHEMA IF NOT EXISTS mart;   -- views / materialized views that Power BI reads
CREATE SCHEMA IF NOT EXISTS audit;  -- pipeline runs, data-quality results, known data gaps

COMMENT ON SCHEMA stg   IS 'Per-interval staging loads from the lake curated zone';
COMMENT ON SCHEMA dw    IS 'Star schema: facts and conformed dimensions';
COMMENT ON SCHEMA mart  IS 'BI-facing views; Power BI reads only mart and audit';
COMMENT ON SCHEMA audit IS 'Operational metadata: pipeline_run, dq_result, data_gap';

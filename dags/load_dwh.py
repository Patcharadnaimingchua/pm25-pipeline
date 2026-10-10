"""Load curated/staging Parquet into the PostgreSQL star schema (dw.*) and refresh the BI views (mart.*).

Manual trigger after transform_curated is green. The load is one database transaction and uses upserts on the
natural keys, so re-running it never duplicates rows; a failed run changes nothing. After loading, row counts
lake vs warehouse are compared (audit.load_reconciliation) and the task fails if any table does not match.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pm25.airflow_helpers import DEFAULT_ARGS, run_context


@dag(
    dag_id="load_dwh",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1, "execution_timeout": timedelta(hours=2)},
    tags=["spark", "dwh", "postgres"],
    doc_md=__doc__,
)
def load_dwh():
    @task(task_id="load_star_schema")
    def load() -> dict:
        from pm25.audit.run_logger import audited_run
        from pm25.load.dwh import run_load
        from pm25.settings import get_settings
        from pm25.spark.session import get_spark, lake_uri

        dsn = get_settings().dwh_dsn
        if not dsn:
            raise RuntimeError("DWH_DSN is not set (see .env.example)")
        rc = run_context(get_current_context())
        with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], "dwh",
                         rc["interval_start"], rc["interval_end"]) as stats:
            spark = get_spark("load_dwh")
            try:
                out = run_load(spark, lake_uri(), dsn)
            finally:
                spark.stop()
            stats.update(records_in=sum(out["lake_rows"].values()), records_out=sum(out["dw_rows"].values()),
                         records_quarantined=0, records_duplicate=0)
        bad = [t for t, ok in out["reconciled"].items() if not ok]
        if bad:
            raise RuntimeError(f"row counts differ between lake and warehouse for: {bad}")
        return out

    load()


load_dwh()

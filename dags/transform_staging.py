"""Spark: raw -> staging (typed, UTC, deduplicated Parquet) + quarantine, for all four sources.

Manual trigger during the backfill week (each run rebuilds staging from all of raw, so it is
idempotent: the same raw always gives the same staging). Tasks run one at a time because each
starts a local Spark session inside the worker container.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pm25.airflow_helpers import DEFAULT_ARGS, run_context

DATASETS = ["air_quality", "hotspot", "weather", "gistda"]
SOURCE = {"air_quality": "openaq", "hotspot": "firms", "weather": "openmeteo", "gistda": "gistda"}


@dag(
    dag_id="transform_staging",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={**DEFAULT_ARGS, "retries": 1, "execution_timeout": timedelta(hours=2)},
    tags=["spark", "staging", "dq"],
    doc_md=__doc__,
)
def transform_staging():
    for name in DATASETS:

        @task(task_id=f"stg_{name}")
        def run_job(dataset: str = name) -> dict:
            from pm25.audit.run_logger import audited_run, record_dq
            from pm25.spark import staging
            from pm25.spark.session import get_spark, lake_uri

            rc = run_context(get_current_context())
            with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], SOURCE[dataset],
                             rc["interval_start"], rc["interval_end"]) as stats:
                spark = get_spark(f"stg_{dataset}")
                try:
                    out = staging.JOBS[dataset](spark, lake_uri())
                finally:
                    spark.stop()
                stats.update(records_in=out["records_in"], records_out=out["records_out"],
                             records_quarantined=out["records_quarantined"],
                             records_duplicate=out["records_duplicate"])
            for rule, n in out["quarantine_by_rule"].items():
                record_dq(stats["pipeline_run_id"], rule, f"staging.{dataset}", "QUARANTINE", False,
                          f"{n} records quarantined", records_checked=out["records_in"], records_failed=n)
            record_dq(stats["pipeline_run_id"], "DQ-11", f"staging.{dataset}", "FAIL_RUN", True,
                      f"{out['records_duplicate']} duplicates removed by business key",
                      records_checked=out["records_in"], records_failed=0)
            return out

        run_job()


transform_staging()

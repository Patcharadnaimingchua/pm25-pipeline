"""Spark: staging -> curated (station-hour with rolling 24 h, station-day, hotspot exposure 50/100/200 km).

Manual trigger after transform_staging is green. Each task rebuilds its dataset from its inputs, so
re-running gives the same result (idempotent). Order matters: station_day needs station_hour and
hotspot_station_day needs station_day.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pm25.airflow_helpers import DEFAULT_ARGS, run_context

DATASETS = ["station_hour", "station_day", "hotspot_station_day"]


@dag(
    dag_id="transform_curated",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={**DEFAULT_ARGS, "retries": 1, "execution_timeout": timedelta(hours=2)},
    tags=["spark", "curated"],
    doc_md=__doc__,
)
def transform_curated():
    previous = None
    for name in DATASETS:

        @task(task_id=f"cur_{name}")
        def run_job(dataset: str = name) -> dict:
            from pm25.audit.run_logger import audited_run
            from pm25.spark import curated
            from pm25.spark.session import get_spark, lake_uri

            rc = run_context(get_current_context())
            with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], "curated",
                             rc["interval_start"], rc["interval_end"]) as stats:
                spark = get_spark(f"cur_{dataset}")
                try:
                    out = curated.JOBS[dataset](spark, lake_uri())
                finally:
                    spark.stop()
                stats.update(records_in=out["records_in"], records_out=out["records_out"],
                             records_quarantined=0, records_duplicate=0)
            return {k: v for k, v in out.items() if k != "quarantine_by_rule"}

        current = run_job()
        if previous is not None:
            previous >> current
        previous = current


transform_curated()

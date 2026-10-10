"""Daily landing of the OpenAQ archive files (Air4Thai stations) for one UTC day + 2 days lookback.

Interval = one UTC day [D 00:00, D+1 00:00), runs shortly after it closes. catchup=True: a missed
day is re-run by the scheduler, and `airflow dags backfill` works for any range. History before
start_date is loaded by scripts/backfill/run.py (same raw layout).
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from pm25.airflow_helpers import DEFAULT_ARGS, run_context


@dag(
    dag_id="ingest_openaq_daily",
    schedule=CronDataIntervalTimetable("15 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=True,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "openaq", "raw", "daily"],
    doc_md=__doc__,
)
def ingest_openaq_daily():
    @task(execution_timeout=timedelta(minutes=45))
    def land_raw() -> dict:
        from pm25.audit.run_logger import audited_run
        from pm25.ingestion.polite import load_policies
        from pm25.lake.s3 import lake_client
        from pm25.pipelines.daily_landing import land_openaq_days, load_locations
        from pm25.settings import get_settings

        rc = run_context(get_current_context())
        policy = load_policies()[0]["openaq-data-archive.s3.amazonaws.com"]
        with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], "openaq",
                         rc["interval_start"], rc["interval_end"]) as stats:
            summary = land_openaq_days(
                lake=lake_client(), bucket=get_settings().lake_bucket,
                run_key=f"{rc['run_id']}__try{rc['try_number']}",
                interval_start=rc["interval_start"], interval_end=rc["interval_end"],
                location_ids=[int(r["location_id"]) for r in load_locations("Air4Thai")],
                rate_per_sec=policy.rate_per_sec,
            )
            stats.update(api_calls=summary["files"] + summary["missing_files"], bytes_in=summary["bytes"],
                         records_in=summary["ok"], records_out=summary["ok"], raw_prefix=summary["raw_prefix"])
        return summary

    land_raw()


ingest_openaq_daily()

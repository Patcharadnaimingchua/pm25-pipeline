"""Daily landing of NASA FIRMS VIIRS NRT detections (Thailand bbox), 5-day overlapping window.

Each run asks for the interval day and the 4 days before it, because NRT files fill in over a few
hours/days. Overlap is intended; staging deduplicates by detection key. Science-quality (SP) data
replaces NRT later via the backfill script.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from pm25.airflow_helpers import DEFAULT_ARGS, run_context

SOURCES = ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "VIIRS_NOAA21_NRT"]


@dag(
    dag_id="ingest_firms_daily",
    schedule=CronDataIntervalTimetable("30 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "firms", "raw", "daily"],
    doc_md=__doc__,
)
def ingest_firms_daily():
    @task(execution_timeout=timedelta(minutes=20))
    def land_raw() -> dict:
        from pm25.audit.run_logger import audited_run
        from pm25.common.geo import THAILAND_BBOX
        from pm25.ingestion.polite import default_client
        from pm25.lake.s3 import lake_client
        from pm25.pipelines.daily_landing import land_firms_days
        from pm25.settings import get_settings

        rc = run_context(get_current_context())
        s = get_settings()
        if not s.firms_map_key:
            raise RuntimeError("FIRMS_MAP_KEY is not set in .env")
        with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], "firms",
                         rc["interval_start"], rc["interval_end"]) as stats:
            summary = land_firms_days(
                lake=lake_client(), bucket=s.lake_bucket, run_key=f"{rc['run_id']}__try{rc['try_number']}",
                interval_start=rc["interval_start"], interval_end=rc["interval_end"],
                fetch=default_client(max_budget_wait_s=300).get, map_key=s.firms_map_key,
                sources=SOURCES, bbox=THAILAND_BBOX,
            )
            stats.update(api_calls=summary["files"], bytes_in=summary["bytes"], records_in=summary["records"],
                         records_out=summary["records"], raw_prefix=summary["raw_prefix"])
        return summary

    land_raw()


ingest_firms_daily()

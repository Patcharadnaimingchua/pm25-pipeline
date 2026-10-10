"""Daily landing of ERA5 hourly weather (Open-Meteo) for every distinct station grid cell.

ERA5 arrives with a delay of several days, so each run fetches a 3-day window ending 6 days before
the interval day (VERIFY the delay and tune lag_days). One request per grid cell, not per station.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from pm25.airflow_helpers import DEFAULT_ARGS, run_context


@dag(
    dag_id="ingest_weather_daily",
    schedule=CronDataIntervalTimetable("45 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "openmeteo", "raw", "daily"],
    doc_md=__doc__,
)
def ingest_weather_daily():
    @task(execution_timeout=timedelta(minutes=30))
    def land_raw() -> dict:
        from pm25.audit.run_logger import audited_run
        from pm25.ingestion.polite import default_client
        from pm25.lake.s3 import lake_client
        from pm25.pipelines.daily_landing import land_weather_days, weather_points
        from pm25.settings import get_settings

        rc = run_context(get_current_context())
        points, _ = weather_points()
        with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], "openmeteo",
                         rc["interval_start"], rc["interval_end"]) as stats:
            summary = land_weather_days(
                lake=lake_client(), bucket=get_settings().lake_bucket,
                run_key=f"{rc['run_id']}__try{rc['try_number']}",
                interval_start=rc["interval_start"], interval_end=rc["interval_end"],
                fetch=default_client(max_budget_wait_s=300).get, points=points,
            )
            stats.update(api_calls=summary["files"], bytes_in=summary["bytes"], records_in=summary["records"],
                         records_out=summary["records"], raw_prefix=summary["raw_prefix"])
        return summary

    land_raw()


ingest_weather_daily()

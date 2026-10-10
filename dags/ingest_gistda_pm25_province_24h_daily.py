"""Daily safety net for GISTDA: the last 24 hourly values of every province.

Runs at 00:40 Thai time (17:40 UTC). If the hourly DAG missed hours, province-level values for
the previous 24 hours are still captured here. District-level gaps older than 24h cannot be
recovered — they are recorded as known gaps (audit.data_gap) in Phase 4.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pm25.airflow_helpers import DEFAULT_ARGS, run_context

SOURCE = "gistda"


@dag(
    dag_id="ingest_gistda_pm25_province_24h_daily",
    schedule="40 17 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "gistda", "raw", "daily", "safety-net"],
    doc_md=__doc__,
)
def ingest_gistda_pm25_province_24h_daily():
    @task(execution_timeout=timedelta(minutes=20))
    def land_raw() -> dict:
        from pm25.audit.run_logger import audited_run
        from pm25.ingestion import gistda
        from pm25.ingestion.polite import default_client
        from pm25.lake.s3 import lake_client
        from pm25.pipelines.gistda_landing import land_province_24h
        from pm25.settings import get_settings

        rc = run_context(get_current_context())
        with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], SOURCE,
                         rc["interval_start"], rc["interval_end"]) as stats:
            client = default_client(max_budget_wait_s=300)  # never block a worker > 5 min
            codes = gistda.province_codes(gistda.parse_province(client.get(gistda.province_url()).body))
            summary = land_province_24h(
                client=lake_client(), bucket=get_settings().lake_bucket,
                run_id=rc["run_id"], try_number=rc["try_number"],
                interval_start=rc["interval_start"], interval_end=rc["interval_end"],
                province_codes=codes, fetch=client.get,
            )
            stats.update(api_calls=summary["files"] + 1, bytes_in=summary["bytes"],
                         records_in=summary["records"], records_out=summary["records"])
            if summary["failed"]:
                raise RuntimeError(f"{summary['failed']} province requests failed — retrying")
        return summary

    land_raw()


ingest_gistda_pm25_province_24h_daily()

"""Hourly landing of GISTDA modeled PM2.5 (province + all districts) into the raw zone.

Why this DAG is switched on first: GISTDA keeps only ~24 hours of history. Every hour this DAG
does not run is data the project can never get back. Turn it on in Week 1 and keep it on.

Schedule: minute 20 of every hour (GISTDA updates hourly). catchup=False on purpose — a backfill
cannot recover data the API no longer serves; gaps are recovered by the daily 24h DAG instead.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pm25.airflow_helpers import DEFAULT_ARGS, run_context

SOURCE = "gistda"


@dag(
    dag_id="ingest_gistda_pm25_hourly",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "gistda", "raw", "hourly"],
    doc_md=__doc__,
)
def ingest_gistda_pm25_hourly():
    @task(execution_timeout=timedelta(minutes=20))
    def land_raw() -> dict:
        from pm25.audit.run_logger import audited_run
        from pm25.ingestion.polite import default_client
        from pm25.lake.s3 import lake_client
        from pm25.pipelines.gistda_landing import land_hour
        from pm25.settings import get_settings

        rc = run_context(get_current_context())
        with audited_run(rc["dag_id"], rc["task_id"], rc["run_id"], rc["try_number"], SOURCE,
                         rc["interval_start"], rc["interval_end"]) as stats:
            summary = land_hour(
                client=lake_client(), bucket=get_settings().lake_bucket,
                run_id=rc["run_id"], try_number=rc["try_number"],
                interval_start=rc["interval_start"], interval_end=rc["interval_end"],
                fetch=default_client(max_budget_wait_s=300).get,  # paced, budgeted, logged; never blocks > 5 min
            )
            stats.update(http_status=200, api_calls=summary["files"], bytes_in=summary["bytes"],
                         records_in=summary["records"], records_out=summary["records"],
                         raw_prefix=summary["raw_prefix"])
        return {"manifest_key": summary["manifest_key"], "pipeline_run_id": stats["pipeline_run_id"]}

    @task(retries=0)
    def dq_raw(landed: dict) -> None:
        from pm25.audit.run_logger import record_dq
        from pm25.lake.s3 import get_bytes, lake_client
        from pm25.pipelines.gistda_landing import check_manifest
        from pm25.settings import get_settings

        manifest = json.loads(get_bytes(lake_client(), get_settings().lake_bucket, landed["manifest_key"]))
        issues = check_manifest(manifest)
        for rule_id, severity, message in issues:
            record_dq(landed["pipeline_run_id"], rule_id, "raw.gistda_pm25", severity, False, message)
        if not issues:
            record_dq(landed["pipeline_run_id"], "DQ-01", "raw.gistda_pm25", "FAIL_RUN", True, "ok")
        fatal = [i for i in issues if i[1] == "FAIL_RUN"]
        if fatal:
            raise RuntimeError(f"raw DQ failed: {fatal}")

    dq_raw(land_raw())


ingest_gistda_pm25_hourly()

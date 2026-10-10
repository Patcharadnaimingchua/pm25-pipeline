"""One-click run of the whole transform + load chain: transform_staging -> transform_curated -> load_dwh.

Trigger this single DAG and it starts the three DAGs in order, waiting for each to finish (success)
before starting the next. If one fails, the chain stops there and the later DAGs are not started.

Ingest DAGs are NOT included on purpose: they pull from external APIs (quota-limited) and run on their
own daily schedules. The data already in the lake is what the three stages below re-process. All three
stages are idempotent, so running this twice gives the same result.

Requirement: transform_staging, transform_curated and load_dwh must be un-paused in the Airflow UI.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

STAGES = ["transform_staging", "transform_curated", "load_dwh"]


@dag(
    dag_id="run_all",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["orchestration"],
    doc_md=__doc__,
)
def run_all():
    previous = None
    for dag_id in STAGES:
        step = TriggerDagRunOperator(
            task_id=f"run_{dag_id}",
            trigger_dag_id=dag_id,
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
            execution_timeout=timedelta(hours=3),
        )
        if previous is not None:
            previous >> step
        previous = step


run_all()

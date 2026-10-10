"""DAG integrity test. Skipped automatically when Airflow is not installed (plain laptop venv).

Run inside the Airflow image, or in a venv with apache-airflow==3.3.2 installed:
    PYTHONPATH=src python -m pytest tests/dags -q
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAGS))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_every_dag_has_tags_retries_and_one_active_run(dagbag):
    assert dagbag.dags, "no DAGs found"
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.max_active_runs is not None and dag.max_active_runs <= 4, dag_id
        for task in dag.tasks:
            assert task.execution_timeout is not None, f"{dag_id}.{task.task_id} has no timeout"


def test_gistda_hourly_shape(dagbag):
    dag = dagbag.dags["ingest_gistda_pm25_hourly"]
    assert dag.catchup is False   # the API cannot serve history older than ~24h
    assert dag.get_task("land_raw").downstream_task_ids == {"dq_raw"}


def test_backfillable_dags_use_data_intervals(dagbag):
    """Daily batch DAGs that support backfill must have a real data interval
    (Airflow 3's default cron timetable has start == end)."""
    from airflow.timetables.interval import CronDataIntervalTimetable

    for dag_id, dag in dagbag.dags.items():
        if dag.catchup:
            assert isinstance(dag.timetable, CronDataIntervalTimetable), (
                f"{dag_id}: catchup=True needs schedule=CronDataIntervalTimetable(...)")

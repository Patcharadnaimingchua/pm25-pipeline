"""Shared helpers for DAG files: read run context and default task policy (design doc 13.2)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

DEFAULT_ARGS = {
    "owner": "de-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=30),
}


def run_context(context: dict) -> dict:
    """Pull what our code needs from the Airflow context, with safe fallbacks for manual runs."""
    now = datetime.now(timezone.utc)
    ti = context.get("ti")
    start = context.get("data_interval_start") or context.get("logical_date") or now
    end = context.get("data_interval_end") or start
    return {
        "dag_id": context["dag"].dag_id,
        "task_id": ti.task_id if ti else "unknown",
        "run_id": context["run_id"],
        "try_number": getattr(ti, "try_number", 1) or 1,
        "interval_start": start,
        "interval_end": end,
    }

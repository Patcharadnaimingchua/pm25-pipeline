"""Write one row per task attempt to audit.pipeline_run (design doc section 21).

Usage inside an Airflow task:

    with audited_run(dag_id, task_id, run_id, try_number, "gistda", start, end) as stats:
        ...
        stats["records_in"] = 1234

If DWH_DSN is not set (unit tests, laptop scripts) the row is only logged.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime

from pm25.common.timeutil import utc_now
from pm25.settings import get_settings

log = logging.getLogger(__name__)

_STAT_FIELDS = (
    "http_status",
    "api_calls",
    "bytes_in",
    "records_in",
    "records_out",
    "records_quarantined",
    "records_duplicate",
    "raw_prefix",
)

_INSERT = """
INSERT INTO audit.pipeline_run
  (pipeline_run_id, dag_id, task_id, source_system, data_interval_start, data_interval_end,
   started_at, status)
VALUES (%(pipeline_run_id)s, %(dag_id)s, %(task_id)s, %(source_system)s, %(data_interval_start)s,
        %(data_interval_end)s, %(started_at)s, 'running')
ON CONFLICT (pipeline_run_id) DO UPDATE
  SET started_at = EXCLUDED.started_at, status = 'running', ended_at = NULL, error_message = NULL
"""

_UPDATE = """
UPDATE audit.pipeline_run SET
  ended_at = %(ended_at)s, status = %(status)s, error_message = %(error_message)s,
  http_status = %(http_status)s, api_calls = %(api_calls)s, bytes_in = %(bytes_in)s,
  records_in = %(records_in)s, records_out = %(records_out)s,
  records_quarantined = %(records_quarantined)s, records_duplicate = %(records_duplicate)s,
  raw_prefix = %(raw_prefix)s
WHERE pipeline_run_id = %(pipeline_run_id)s
"""


def make_pipeline_run_id(dag_id: str, task_id: str, run_id: str, try_number: int) -> str:
    return f"{dag_id}__{task_id}__{run_id}__try{try_number}"


def _execute(sql: str, params: dict) -> None:
    dsn = get_settings().dwh_dsn
    if not dsn:
        log.info("audit (no DWH_DSN, log only): %s", json.dumps(params, default=str))
        return
    import psycopg  # imported lazily so unit tests do not need a database driver

    with psycopg.connect(dsn, connect_timeout=10) as conn:
        conn.execute(sql, params)


@contextmanager
def audited_run(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    source_system: str,
    data_interval_start: datetime,
    data_interval_end: datetime,
):
    pipeline_run_id = make_pipeline_run_id(dag_id, task_id, run_id, try_number)
    base = {
        "pipeline_run_id": pipeline_run_id,
        "dag_id": dag_id,
        "task_id": task_id,
        "source_system": source_system,
        "data_interval_start": data_interval_start,
        "data_interval_end": data_interval_end,
        "started_at": utc_now(),
    }
    try:
        _execute(_INSERT, base)
    except Exception:  # a warehouse outage must not stop ingestion (GISTDA keeps only 24h of history)
        log.exception("could not write audit start row for %s", pipeline_run_id)
    stats: dict = {"pipeline_run_id": pipeline_run_id}
    try:
        yield stats
    except Exception as exc:
        _finish(pipeline_run_id, stats, "failed", f"{type(exc).__name__}: {exc}"[:2000])
        raise
    else:
        _finish(pipeline_run_id, stats, "success", None)


def _finish(pipeline_run_id: str, stats: dict, status: str, error: str | None) -> None:
    params = {f: stats.get(f) for f in _STAT_FIELDS}
    params.update(pipeline_run_id=pipeline_run_id, ended_at=utc_now(), status=status, error_message=error)
    try:
        _execute(_UPDATE, params)
    except Exception:  # auditing must never hide the original task error
        log.exception("could not write audit row for %s", pipeline_run_id)


_DQ_INSERT = """
INSERT INTO audit.dq_result
  (pipeline_run_id, rule_id, dataset, severity, records_checked, records_failed, passed, message)
VALUES (%(pipeline_run_id)s, %(rule_id)s, %(dataset)s, %(severity)s, %(records_checked)s,
        %(records_failed)s, %(passed)s, %(message)s)
"""


def record_dq(pipeline_run_id: str, rule_id: str, dataset: str, severity: str, passed: bool,
              message: str = "", records_checked: int | None = None, records_failed: int | None = None) -> None:
    params = dict(pipeline_run_id=pipeline_run_id, rule_id=rule_id, dataset=dataset, severity=severity,
                  records_checked=records_checked, records_failed=records_failed, passed=passed,
                  message=message[:2000])
    try:
        _execute(_DQ_INSERT, params)
    except Exception:
        log.exception("could not write dq_result %s for %s", rule_id, pipeline_run_id)

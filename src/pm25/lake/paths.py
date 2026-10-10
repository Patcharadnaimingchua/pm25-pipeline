"""Object-key layout of the data lake (design doc section 10).

raw/      partitioned by INGESTION date + run_id   -> immutable, never overwritten
staging/  partitioned by EVENT date                 -> Parquet, rebuilt from raw
curated/  partitioned by EVENT month                -> Parquet, rebuilt from staging
"""

from __future__ import annotations

import re
from datetime import date

_SAFE = re.compile(r"[^A-Za-z0-9._=-]+")
_VALID_PART = re.compile(r"^[a-z0-9_]+$")


def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':' and '+' (e.g. scheduled__2026-10-02T10:20:00+00:00).
    Keep them readable but safe as an S3 path segment."""
    return _SAFE.sub("_", run_id).strip("_")


def _check(name: str, value: str) -> None:
    if not _VALID_PART.match(value):
        raise ValueError(f"{name} must be lowercase snake_case, got {value!r}")


def raw_prefix(source: str, dataset: str, ingest_date: date, run_id: str) -> str:
    _check("source", source)
    _check("dataset", dataset)
    return (
        f"raw/source={source}/dataset={dataset}/"
        f"ingest_date={ingest_date.isoformat()}/run_id={safe_run_id(run_id)}/"
    )


def raw_key(source: str, dataset: str, ingest_date: date, run_id: str, filename: str) -> str:
    if "/" in filename or not filename:
        raise ValueError(f"filename must be a plain name, got {filename!r}")
    return raw_prefix(source, dataset, ingest_date, run_id) + filename


def manifest_key(source: str, dataset: str, ingest_date: date, run_id: str) -> str:
    return raw_key(source, dataset, ingest_date, run_id, "_manifest.json")


def staging_prefix(dataset: str) -> str:
    _check("dataset", dataset)
    return f"staging/{dataset}/"


def curated_prefix(dataset: str) -> str:
    _check("dataset", dataset)
    return f"curated/{dataset}/"


def quarantine_prefix(dataset: str) -> str:
    _check("dataset", dataset)
    return f"quarantine/{dataset}/"

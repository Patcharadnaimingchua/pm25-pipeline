"""Runtime settings read from environment variables (.env in development).

Secrets never live in code or in config/sources.yaml.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

_PROJECT_CONFIG_CANDIDATES = (
    Path("/opt/airflow/project-config/sources.yaml"),  # inside Docker
    Path(__file__).resolve().parents[2] / "config" / "sources.yaml",  # local checkout
)


@dataclass(frozen=True)
class Settings:
    s3_endpoint_url: str | None
    lake_bucket: str
    dwh_dsn: str | None
    firms_map_key: str | None
    openaq_api_key: str | None
    user_agent: str


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name, default)
    return value if value not in ("", None) else default


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        s3_endpoint_url=_env("S3_ENDPOINT_URL"),
        lake_bucket=_env("LAKE_BUCKET", "pm25-lake"),
        dwh_dsn=_env("DWH_DSN"),
        firms_map_key=_env("FIRMS_MAP_KEY"),
        openaq_api_key=_env("OPENAQ_API_KEY"),
        user_agent=_env("HTTP_USER_AGENT", "pm25-thailand-student-pipeline/0.1"),
    )


@lru_cache(maxsize=1)
def load_sources_config() -> dict:
    for path in _PROJECT_CONFIG_CANDIDATES:
        if path.exists():
            return yaml.safe_load(path.read_text(encoding="utf-8"))
    raise FileNotFoundError("config/sources.yaml not found")

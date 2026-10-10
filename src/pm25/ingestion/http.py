"""HTTP fetching with an explicit split between retryable and fatal errors.

Airflow does the retrying (retries + exponential backoff on the task). Our job is to make
sure a wrong API key fails fast instead of being retried for an hour.
"""

from __future__ import annotations

from dataclasses import dataclass

import requests

from pm25.settings import get_settings

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class RetryableHttpError(RuntimeError):
    """Timeouts, 429 and 5xx: worth retrying later."""


class FatalHttpError(RuntimeError):
    """Other 4xx (bad key, bad parameters): retrying will not help."""


@dataclass
class HttpResult:
    url: str
    status: int
    body: bytes
    content_type: str


def http_get(url: str, *, params: dict | None = None, headers: dict | None = None,
             timeout: float = 30.0, session: requests.Session | None = None) -> HttpResult:
    hdrs = {"User-Agent": get_settings().user_agent}
    hdrs.update(headers or {})
    getter = session.get if session is not None else requests.get
    try:
        resp = getter(url, params=params, headers=hdrs, timeout=timeout)
    except (requests.Timeout, requests.ConnectionError) as exc:
        raise RetryableHttpError(f"{type(exc).__name__} for {url}") from exc
    if resp.status_code in RETRYABLE_STATUS:
        raise RetryableHttpError(f"HTTP {resp.status_code} for {resp.url}")
    if resp.status_code >= 400:
        raise FatalHttpError(f"HTTP {resp.status_code} for {resp.url}: {resp.text[:200]}")
    return HttpResult(url=resp.url, status=resp.status_code, body=resp.content,
                      content_type=resp.headers.get("Content-Type", "application/octet-stream"))

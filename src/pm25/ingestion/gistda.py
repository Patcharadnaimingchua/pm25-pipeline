"""GISTDA PM2.5 (ระบบเช็คฝุ่น) — modeled PM2.5 by province / district.

Verified 2026-10-02:
  GET /rest/getPm25byProvince            -> {status, errMsg, data[77], datetimeThai, datetimeEng}
      data[i] = {pv_tn, pv_en, pv_idn, pm25, dt, pm25Avg24hr}, dt like "2026-10-02T18:00:00.000Z"
  GET /rest/getPm25byAmphoe?pv_idn=50    -> data[25] = {ap_tn, ap_en, ap_idn, pm25, dt, pm25Avg24hr}
  GET /rest/getPM25byProvince24hrs?pv_idn=10 -> graphHistory24hrs = [[pm25, dt], ...] x 24,
      graphMetadata = ["pm25", "dt"]

Values are model estimates (satellite + ground stations), not station measurements.
The API keeps only ~24 hours of history, so every missed hour older than that is lost.

TIMEZONE QUIRK (verified 2026-10-02, three observations):
  `dt` carries a "Z" (UTC) suffix but is Thai local time (Asia/Bangkok, +07:00).
  - 17:52 ICT (10:52 UTC): newest 24h point was "17:00:00.000Z" and timeThai said "17:00 น."
  - 19:06 ICT (12:06 UTC): newest province dt was "18:00:00.000Z" — 6 hours in the future if it were UTC
  So we ignore the suffix, read the clock time as ICT and convert to UTC. check_dt_plausible()
  guards against GISTDA fixing the API later (then our reading would land ~7h in the past).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

from pm25.common.timeutil import ICT, UTC

BASE_URL = "https://pm25.gistda.or.th/rest"


class GistdaPayloadError(ValueError):
    pass


def province_url() -> str:
    return f"{BASE_URL}/getPm25byProvince"


def amphoe_url(pv_idn: int) -> str:
    return f"{BASE_URL}/getPm25byAmphoe?pv_idn={int(pv_idn)}"


def province_24h_url(pv_idn: int) -> str:
    return f"{BASE_URL}/getPM25byProvince24hrs?pv_idn={int(pv_idn)}"


def parse_gistda_dt(value: str) -> datetime:
    """'2026-10-02T18:00:00.000Z' -> 2026-10-02T11:00:00+00:00 (the clock time is ICT despite the Z)."""
    naive = datetime.fromisoformat(value.rstrip("Z").split("+")[0])
    if naive.tzinfo is not None:
        raise ValueError(f"unexpected offset in GISTDA dt {value!r}")
    return naive.replace(tzinfo=ICT).astimezone(UTC)


def check_dt_plausible(latest_event_ts: datetime, fetched_at: datetime,
                       max_future: timedelta = timedelta(minutes=30),
                       max_lag: timedelta = timedelta(hours=4)) -> str | None:
    """Hourly data fetched now should be from the last few hours. Returns a warning or None.

    Too far in the future -> the ICT reading is wrong (or the clock is off).
    Too old (about 7h)      -> GISTDA may have fixed the suffix; re-check before trusting the data.
    """
    delta = fetched_at - latest_event_ts
    if delta < -max_future:
        return f"newest GISTDA hour is {-delta} in the future (fetched {fetched_at.isoformat()})"
    if delta > max_lag:
        return (f"newest GISTDA hour is {delta} old — if close to 7h, GISTDA may now send real UTC; "
                f"re-verify the dt convention")
    return None


def _load(body: bytes | str) -> dict[str, Any]:
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise GistdaPayloadError(f"not JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise GistdaPayloadError("payload is not a JSON object")
    if payload.get("status") != 200 or payload.get("errMsg"):
        raise GistdaPayloadError(f"API error status={payload.get('status')} errMsg={payload.get('errMsg')!r}")
    return payload


def _num(value: Any) -> float | None:
    return None if value is None else float(value)


def parse_province(body: bytes | str) -> list[dict]:
    payload = _load(body)
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise GistdaPayloadError("province payload has no data")
    return [
        {
            "admin_level": "province",
            "admin_code": int(r["pv_idn"]),
            "parent_code": None,
            "name_th": r.get("pv_tn"),
            "name_en": r.get("pv_en"),
            "pm25_modeled": _num(r.get("pm25")),
            "pm25_avg_24h": _num(r.get("pm25Avg24hr")),
            "event_ts": parse_gistda_dt(r["dt"]),
        }
        for r in rows
    ]


def parse_amphoe(body: bytes | str, pv_idn: int) -> list[dict]:
    payload = _load(body)
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise GistdaPayloadError(f"amphoe payload for pv_idn={pv_idn} has no data")
    return [
        {
            "admin_level": "amphoe",
            "admin_code": int(r["ap_idn"]),
            "parent_code": int(pv_idn),
            "name_th": r.get("ap_tn"),
            "name_en": r.get("ap_en"),
            "pm25_modeled": _num(r.get("pm25")),
            "pm25_avg_24h": _num(r.get("pm25Avg24hr")),
            "event_ts": parse_gistda_dt(r["dt"]),
        }
        for r in rows
    ]


def parse_24h(body: bytes | str, admin_level: str, admin_code: int) -> list[dict]:
    payload = _load(body)
    meta = payload.get("graphMetadata")
    if meta != ["pm25", "dt"]:
        raise GistdaPayloadError(f"unexpected graphMetadata {meta!r}")
    points = payload.get("graphHistory24hrs") or []
    return [
        {
            "admin_level": admin_level,
            "admin_code": int(admin_code),
            "pm25_modeled": _num(value),
            "event_ts": parse_gistda_dt(dt),
        }
        for value, dt in points
    ]


def province_codes(province_rows: list[dict]) -> list[int]:
    return sorted({r["admin_code"] for r in province_rows})

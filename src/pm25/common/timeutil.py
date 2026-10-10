"""Time helpers. Rule: store and compute in UTC; convert to Thai local time only for
calendar concepts (the 'day' in daily aggregates, the hour-of-day on dashboards)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

UTC = timezone.utc
ICT = timezone(timedelta(hours=7), name="Asia/Bangkok")  # Thailand has no DST


def parse_iso_utc(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (with 'Z' or an offset) into an aware UTC datetime.

    Naive timestamps are rejected on purpose: guessing a timezone is how hour shifts happen.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"timestamp has no timezone: {value!r}")
    return dt.astimezone(UTC)


def local_date(dt_utc: datetime) -> date:
    """Thai calendar date of a UTC instant."""
    if dt_utc.tzinfo is None:
        raise ValueError("expected an aware datetime")
    return dt_utc.astimezone(ICT).date()


def floor_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def utc_now() -> datetime:
    return datetime.now(UTC)


def date_range(start: date, end: date):
    """Inclusive range of dates."""
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)

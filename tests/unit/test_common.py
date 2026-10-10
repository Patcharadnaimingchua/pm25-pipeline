from datetime import date, datetime, timezone

import pytest

from pm25.common.geo import THAILAND_BBOX, grid_cell, haversine_km
from pm25.common.timeutil import date_range, local_date, parse_iso_utc


def test_parse_z_and_offset_to_utc():
    a = parse_iso_utc("2026-10-02T18:00:00.000Z")
    b = parse_iso_utc("2026-10-03T01:00:00+07:00")
    assert a == b
    assert a.tzinfo == timezone.utc


def test_naive_timestamp_rejected():
    with pytest.raises(ValueError):
        parse_iso_utc("2026-10-02T18:00:00")


def test_local_date_crosses_midnight():
    # 18:00 UTC is 01:00 the next day in Thailand
    assert local_date(datetime(2026, 10, 1, 18, tzinfo=timezone.utc)) == date(2026, 10, 2)
    assert local_date(datetime(2026, 10, 1, 16, 59, tzinfo=timezone.utc)) == date(2026, 10, 1)


def test_analysis_window_hours():
    days = list(date_range(date(2022, 1, 1), date(2025, 12, 31)))
    assert len(days) == 1461
    assert len(days) * 24 == 35_064  # used in design doc 7.5


def test_haversine_one_degree_latitude():
    assert haversine_km(18.0, 99.0, 19.0, 99.0) == pytest.approx(111.19, abs=0.05)


def test_haversine_symmetric_and_zero():
    assert haversine_km(13.75, 100.5, 18.79, 98.98) == pytest.approx(haversine_km(18.79, 98.98, 13.75, 100.5))
    assert haversine_km(13.75, 100.5, 13.75, 100.5) == 0


def test_bbox_and_firms_format():
    assert THAILAND_BBOX.contains(13.75, 100.5)       # Bangkok
    assert not THAILAND_BBOX.contains(100.5, 13.75)   # swapped lat/lon (DQ-08)
    assert THAILAND_BBOX.as_firms() == "97.3,5.5,105.7,20.6"


def test_grid_cell_negative_safe():
    assert grid_cell(18.79, 98.98) == (18, 98)
    assert grid_cell(-0.5, -0.5) == (-1, -1)

from datetime import date

import pytest

from pm25.lake import paths


def test_raw_key_layout():
    key = paths.raw_key("gistda", "pm25_amphoe_hourly", date(2026, 10, 2),
                        "scheduled__2026-10-02T10:20:00+00:00__try1", "province.json")
    assert key == (
        "raw/source=gistda/dataset=pm25_amphoe_hourly/ingest_date=2026-10-02/"
        "run_id=scheduled__2026-10-02T10_20_00_00_00__try1/province.json"
    )


def test_run_id_is_path_safe():
    assert "/" not in paths.safe_run_id("manual__2026/10/02")
    assert ":" not in paths.safe_run_id("scheduled__2026-10-02T10:20:00+00:00")


@pytest.mark.parametrize("bad", ["Gistda", "pm2.5", "with space", ""])
def test_rejects_bad_partition_names(bad):
    with pytest.raises(ValueError):
        paths.raw_prefix(bad, "pm25", date(2026, 1, 1), "r")


def test_rejects_nested_filename():
    with pytest.raises(ValueError):
        paths.raw_key("gistda", "pm25", date(2026, 1, 1), "r", "a/b.json")


def test_zone_prefixes():
    assert paths.staging_prefix("air_quality") == "staging/air_quality/"
    assert paths.curated_prefix("station_day") == "curated/station_day/"
    assert paths.quarantine_prefix("hotspot") == "quarantine/hotspot/"

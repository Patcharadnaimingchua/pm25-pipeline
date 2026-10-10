"""Warehouse load against a real PostgreSQL. Skipped unless PM25_TEST_DSN points at a scratch database:

    PM25_TEST_DSN=postgresql://postgres@localhost:55432/pm25test \\
        PYTHONPATH=src python -m pytest tests/spark/test_load_dwh.py
Fixture rows are invented test data in the curated/staging schemas; they are never counted as records.
"""

import os
import shutil
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
psycopg = pytest.importorskip("psycopg")
if not shutil.which("java") or not os.environ.get("PM25_TEST_DSN"):
    pytest.skip("needs java and PM25_TEST_DSN", allow_module_level=True)

from pyspark.sql import functions as F  # noqa: E402

from pm25.load import dwh  # noqa: E402
from pm25.spark import curated  # noqa: E402
from pm25.spark.session import get_spark  # noqa: E402

UTC = timezone.utc
DSN = os.environ["PM25_TEST_DSN"]
WX = ("point_id string, event_ts timestamp, temperature_2m double, relative_humidity_2m double, "
      "wind_speed_10m double, wind_direction_10m double, precipitation double, boundary_layer_height double")


@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests_load", master="local[2]", driver_memory="1g")
    yield s
    s.stop()


@pytest.fixture()
def lake(spark, tmp_path):
    lake = str(tmp_path / "lake")
    start = datetime(2024, 3, 1, 0, 0, tzinfo=UTC) - timedelta(hours=7)          # 00:00 Bangkok
    rows = [("openaq:1", 1, "Station, with comma", start + timedelta(hours=h), 18.8, 98.9, "PM2.5", 20.0 + h)
            for h in range(24)]
    spark.createDataFrame(rows, "station_id string, location_id long, location_name string, event_ts timestamp, "
                                "latitude double, longitude double, pollutant string, value double"
                          ).write.mode("overwrite").parquet(f"{lake}/staging/air_quality")
    spark.createDataFrame([("cell_188_989", start + timedelta(hours=h), 30.0, 60.0, 2.0, 90.0, 0.0, 800.0)
                           for h in range(24)], WX).withColumn("grid_latitude", F.lit(18.8)) \
        .withColumn("grid_longitude", F.lit(98.9)).write.mode("overwrite").parquet(f"{lake}/staging/weather")
    hot = spark.createDataFrame(
        [("h1", "VIIRS_SNPP", "VIIRS", "SP", 18.85, 98.95, start + timedelta(hours=5), "h", 12.0, 330.0, "D", True),
         ("h2", "VIIRS_SNPP", "VIIRS", "SP", 15.0, 100.0, start + timedelta(hours=6), "n", 3.0, 320.0, "N", True)],
        "hotspot_id string, satellite string, instrument string, product string, latitude double, "
        "longitude double, event_ts timestamp, confidence string, frp double, bright_ti4 double, "
        "daynight string, in_thailand_bbox boolean")
    hot.write.mode("overwrite").parquet(f"{lake}/staging/hotspot")
    spark.createDataFrame([("province", 50, None, "เชียงใหม่", "Chiang Mai", start + timedelta(hours=3), 40.0, 35.0)],
                          "admin_level string, admin_code int, parent_code int, name_th string, name_en string, "
                          "event_ts timestamp, pm25_modeled double, pm25_avg_24h double"
                          ).write.mode("overwrite").parquet(f"{lake}/staging/gistda_pm25")
    curated.cur_station_hour(spark, lake)
    curated.cur_station_day(spark, lake)
    curated.cur_hotspot_station_day(spark, lake)
    return lake


def reset():
    with psycopg.connect(DSN) as c:
        for schema in ("dw", "mart", "audit", "stg"):
            c.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def test_load_is_idempotent_and_reconciles(spark, lake, tmp_path):
    reset()
    lk = lake
    first = dwh.run_load(spark, lk, DSN, str(tmp_path / "work"))
    assert all(first["reconciled"].values()), first
    second = dwh.run_load(spark, lk, DSN, str(tmp_path / "work"))          # same lake again
    assert second["dw_rows"] == first["dw_rows"]                                    # no duplicates
    with psycopg.connect(DSN) as c:
        assert c.execute("SELECT count(*) FROM dw.fact_air_quality_hour").fetchone()[0] == 24
        name, = c.execute("SELECT station_name FROM dw.dim_station").fetchone()
        assert name == "Station, with comma"                                        # CSV quoting survived
        (n50, f100, pm25), = c.execute(
            "SELECT hotspots_50km, frp_sum_100km, pm25_mean FROM mart.v_station_day").fetchall()
        assert n50 == 1 and f100 == pytest.approx(12.0) and pm25 == pytest.approx(sum(20.0 + h for h in range(24)) / 24)
        assert c.execute("SELECT count(*) FROM mart.v_hotspot_daily").fetchone()[0] == 1
        cov, = c.execute("SELECT coverage_pct FROM mart.v_pm25_coverage_month").fetchone()
        assert float(cov) == pytest.approx(100.0)       # data range is that one day: 24 of 24 hours, 1 station
        cov, low = c.execute("SELECT coverage_pct, is_low_coverage FROM mart.v_monthly_pm25").fetchone()
        assert float(cov) == pytest.approx(100 / 31, abs=0.1) and low is True            # 1 valid day of 31
        assert c.execute("SELECT name_th FROM dw.dim_admin_area").fetchone()[0] == "เชียงใหม่"
        assert c.execute("SELECT count(*) FROM audit.load_reconciliation WHERE NOT matches").fetchone()[0] == 0
        # hour 5 local: PM2.5 = 25, weather joined
        t, = c.execute("SELECT temperature_2m FROM dw.fact_air_quality_hour WHERE hour_of_day = 5").fetchone()
        assert t == 30.0

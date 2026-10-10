"""Curated Spark jobs on a tiny local lake. Skipped when PySpark/Java are not installed.

    PYTHONPATH=src python -m pytest tests/spark -q
Rows are invented test data in the staging schema; they are never counted as project records.
"""

import random
import shutil
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("java not installed", allow_module_level=True)

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from pm25.common.geo import haversine_km  # noqa: E402
from pm25.spark import curated  # noqa: E402
from pm25.spark.session import get_spark  # noqa: E402

UTC = timezone.utc


@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests_curated", master="local[2]", driver_memory="1g")
    yield s
    s.stop()


def aq_schema():
    return T.StructType([
        T.StructField("station_id", T.StringType()), T.StructField("location_id", T.LongType()),
        T.StructField("location_name", T.StringType()), T.StructField("event_ts", T.TimestampType()),
        T.StructField("latitude", T.DoubleType()), T.StructField("longitude", T.DoubleType()),
        T.StructField("pollutant", T.StringType()), T.StructField("value", T.DoubleType())])


def write_staging_aq(spark, lake, rows):
    df = spark.createDataFrame(rows, aq_schema()).withColumn("event_date", F.to_date("event_ts"))
    df.write.mode("overwrite").parquet(f"{lake}/staging/air_quality")


def test_rolling_24h_needs_enough_hours_and_station_day(spark, tmp_path):
    lake = str(tmp_path / "lake")
    t0 = datetime(2024, 3, 1, 0, 0, tzinfo=UTC)          # 07:00 Bangkok
    rows = [("openaq:1", 1, "S1", t0 + timedelta(hours=h), 13.7, 100.5, "PM2.5", float(h)) for h in range(30)]
    rows += [("openaq:1", 1, "S1", t0 + timedelta(hours=0), 13.7, 100.5, "PM10", 40.0)]
    write_staging_aq(spark, lake, rows)
    wx = spark.createDataFrame([("cell_137_1005", t0 + timedelta(hours=h), 30.0, 60.0, 2.0, 90.0, 0.0, 800.0)
                                for h in range(30)],
                               "point_id string, event_ts timestamp, temperature_2m double, "
                               "relative_humidity_2m double, wind_speed_10m double, wind_direction_10m double, "
                               "precipitation double, boundary_layer_height double")
    wx.write.mode("overwrite").parquet(f"{lake}/staging/weather")

    out = curated.cur_station_hour(spark, lake)
    assert out["records_out"] == 30 and out["duplicate_keys"] == 0
    sh = {r.h: r for r in spark.read.parquet(f"{lake}/curated/station_hour")
          .withColumn("h", F.hour("hour_ts") + 24 * F.dayofmonth("hour_ts") - 24).collect()}
    assert sh[5].pm25_24h_n == 6 and sh[5].pm25_24h_mean is None          # only 6 of 24 hours -> no value
    assert sh[23].pm25_24h_n == 24 and sh[23].pm25_24h_mean == pytest.approx(11.5)
    assert sh[29].pm25_24h_n == 24 and sh[29].pm25_24h_mean == pytest.approx(sum(range(6, 30)) / 24)
    assert sh[0].pm10 == 40.0 and sh[0].temperature_2m == 30.0           # weather joined by station point

    curated.cur_station_day(spark, lake)
    days = {str(r.local_date): r for r in spark.read.parquet(f"{lake}/curated/station_day").collect()}
    # Bangkok day 2024-03-01 = UTC 2024-02-29 17:00 .. 2024-03-01 16:59 -> hours 0..16 only -> 17 < 18 -> NULL
    assert days["2024-03-01"].pm25_hours == 17 and days["2024-03-01"].pm25_mean is None
    assert days["2024-03-02"].pm25_hours == 13 and days["2024-03-02"].pm25_mean is None
    assert days["2024-03-01"].temp_mean == pytest.approx(30.0)


def test_station_day_valid_when_complete(spark, tmp_path):
    lake = str(tmp_path / "lake")
    t0 = datetime(2024, 3, 1, 0, 0, tzinfo=UTC)         # UTC midnight = 07:00 Bangkok; local day 03-01 is h 0..16
    start = t0 - timedelta(hours=7)                       # 00:00 Bangkok
    rows = [("openaq:1", 1, "S1", start + timedelta(hours=h), 13.7, 100.5, "PM2.5", 10.0 + h) for h in range(24)]
    write_staging_aq(spark, lake, rows)
    spark.createDataFrame([], "point_id string, event_ts timestamp, temperature_2m double, "
                              "relative_humidity_2m double, wind_speed_10m double, wind_direction_10m double, "
                              "precipitation double, boundary_layer_height double"
                          ).write.mode("overwrite").parquet(f"{lake}/staging/weather")
    curated.cur_station_hour(spark, lake)
    curated.cur_station_day(spark, lake)
    (r,) = spark.read.parquet(f"{lake}/curated/station_day").collect()
    assert r.pm25_hours == 24 and r.pm25_mean == pytest.approx(sum(10.0 + h for h in range(24)) / 24)
    assert r.temp_mean is None                                            # no weather -> NULL, not 0


def test_hotspot_join_matches_brute_force(spark):
    rnd = random.Random(7)
    stations = [(f"openaq:{i}", rnd.uniform(6.0, 20.4), rnd.uniform(97.6, 105.4)) for i in range(40)]
    day = datetime(2024, 3, 1, 3, 0, tzinfo=UTC)
    hots = []
    for _ in range(3000):
        lat, lon = rnd.uniform(5.6, 20.5), rnd.uniform(97.4, 105.6)
        hots.append((lat, lon, day + timedelta(minutes=rnd.randint(0, 600)), rnd.uniform(1, 50),
                     rnd.choice(["l", "n", "h"])))
    sdf = spark.createDataFrame(stations, "station_id string, latitude double, longitude double")
    hdf = spark.createDataFrame(hots, "latitude double, longitude double, event_ts timestamp, frp double, "
                                     "confidence string")
    got = {(r.station_id, str(r.local_date)): r for r in curated.hotspot_exposure(hdf, sdf).collect()}

    expected = {}
    for sid, slat, slon in stations:
        for hlat, hlon, ts, frp, conf in hots:
            d = haversine_km(slat, slon, hlat, hlon)
            if d > 200:
                continue
            local = (ts + timedelta(hours=7)).date()
            e = expected.setdefault((sid, str(local)), {"n50": 0, "n100": 0, "n200": 0, "f100": 0.0, "hc": 0})
            e["n200"] += 1
            if d <= 100:
                e["n100"] += 1
                e["f100"] += frp
                e["hc"] += conf == "h"
            if d <= 50:
                e["n50"] += 1
    assert set(got) == set(expected) and len(expected) > 0
    for k, e in expected.items():
        g = got[k]
        assert (g["hotspots_50km"], g["hotspots_100km"], g["hotspots_200km"]) == (e["n50"], e["n100"], e["n200"])
        assert g["frp_sum_100km"] == pytest.approx(e["f100"]) and g["hotspots_high_conf_100km"] == e["hc"]


def test_neighbour_span_covers_200km_at_highest_latitude():
    assert curated.neighbour_span(200) == 2
    assert curated.neighbour_span(50) == 1


def test_hotspot_station_day_fills_zero_only_in_firms_coverage(spark, tmp_path):
    lake = str(tmp_path / "lake")
    base = datetime(2024, 3, 1, 0, 0, tzinfo=UTC)
    rows = []
    for d in range(4):                                    # station has PM2.5 on Bangkok days 03-01 .. 03-04
        start = base + timedelta(days=d) - timedelta(hours=7)
        rows += [("openaq:1", 1, "S1", start + timedelta(hours=h), 18.8, 98.9, "PM2.5", 30.0) for h in range(24)]
    write_staging_aq(spark, lake, rows)
    spark.createDataFrame([], "point_id string, event_ts timestamp, temperature_2m double, "
                              "relative_humidity_2m double, wind_speed_10m double, wind_direction_10m double, "
                              "precipitation double, boundary_layer_height double"
                          ).write.mode("overwrite").parquet(f"{lake}/staging/weather")
    curated.cur_station_hour(spark, lake)
    curated.cur_station_day(spark, lake)
    # FIRMS covers only 03-02 and 03-03: one hotspot 10 km away on 03-02, none on 03-03
    h = [(18.85, 98.95, base + timedelta(days=1, hours=3), 12.0, "h", "SP")]
    spark.createDataFrame(h, "latitude double, longitude double, event_ts timestamp, frp double, confidence string, "
                             "product string").withColumn("event_date", F.to_date("event_ts")) \
        .union(spark.createDataFrame([(5.7, 98.0, base + timedelta(days=2, hours=3), 1.0, "n", "SP")],
                                     "latitude double, longitude double, event_ts timestamp, frp double, "
                                     "confidence string, product string")
                       .withColumn("event_date", F.to_date("event_ts"))
               ).write.mode("overwrite").parquet(f"{lake}/staging/hotspot")
    out = curated.cur_hotspot_station_day(spark, lake)
    rows = {str(r.local_date): r for r in spark.read.parquet(f"{lake}/curated/hotspot_station_day").collect()}
    assert set(rows) == {"2024-03-02", "2024-03-03"}                      # 03-01 and 03-04 are outside FIRMS coverage
    assert rows["2024-03-02"].hotspots_50km == 1 and rows["2024-03-02"].frp_sum_100km == pytest.approx(12.0)
    assert rows["2024-03-03"].hotspots_200km == 0 and rows["2024-03-03"].frp_sum_200km == 0.0   # real zero
    assert out["records_out"] == 2


def test_same_site_under_two_location_ids_is_one_station(spark, tmp_path):
    lake = str(tmp_path / "lake")
    t0 = datetime(2024, 3, 1, 0, 0, tzinfo=UTC)
    rows = [("openaq:10", 10, "Site, A-1000", t0 + timedelta(hours=h), 13.7, 100.5, "PM2.5", 10.0) for h in range(3)]
    rows += [("openaq:11", 11, "Site, A-1001", t0 + timedelta(hours=h + 3), 13.7, 100.5, "PM2.5", 12.0)
             for h in range(3)]
    rows += [("openaq:12", 12, "Other, B-2000", t0 + timedelta(hours=h), 15.0, 99.0, "PM2.5", 20.0) for h in range(3)]
    write_staging_aq(spark, lake, rows)
    st = {r.station_id: r for r in curated.station_table(spark, lake).collect()}
    assert set(st) == {"openaq:10", "openaq:12"}
    aq = curated.air_quality_by_station(spark, lake)
    assert {r.source_location_id for r in aq.filter("station_id = 'openaq:10'").collect()} == {10, 11}

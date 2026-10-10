"""Spark staging jobs on a tiny local lake. Skipped when PySpark/Java are not installed.

    PYTHONPATH=src python -m pytest tests/spark -q
Fixture rows are invented test data in the real raw formats; they are never counted as records.
"""

import gzip
import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("java not installed", allow_module_level=True)

from pyspark.sql import functions as F  # noqa: E402

from pm25.spark import staging  # noqa: E402
from pm25.spark.session import get_spark  # noqa: E402

RAW = "raw/source={s}/dataset={d}/ingest_date={day}/run_id={run}/{name}"


def utc(df):
    """Python converts timestamps to the machine's local zone on collect(); format inside Spark (UTC)."""
    return df.withColumn("ts_utc", F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss"))


def put(lake: Path, source, dataset, day, run, name, data: bytes):
    p = lake / RAW.format(s=source, d=dataset, day=day, run=run, name=name)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)


@pytest.fixture(scope="module")
def spark():
    s = get_spark("tests", master="local[2]", driver_memory="1g")
    yield s
    s.stop()


@pytest.fixture()
def lake(tmp_path):
    return tmp_path / "lake"


def test_air_quality_dedup_utc_and_quarantine(spark, lake):
    hdr = "location_id,sensors_id,location,datetime,lat,lon,parameter,units,value\n"
    day1 = (hdr
            + "1,11,BKK,2024-03-01T01:00:00+07:00,13.7,100.5,pm25,µg/m³,20\n"
            + "1,11,BKK,2024-03-01T02:00:00+07:00,13.7,100.5,pm25,µg/m³,-1\n"        # DQ-06
            + "1,12,BKK,2024-03-01T01:00:00+07:00,13.7,100.5,pm10,µg/m³,30\n"
            + "2,21,XX,2024-03-01T01:00:00+07:00,100.5,13.7,pm25,µg/m³,15\n")        # DQ-08 swapped lat/lon
    put(lake, "openaq", "measurements", "2026-10-02", "a", "locationid=1__location-1-20240301.csv.gz",
        gzip.compress(day1.encode()))
    # same reading delivered again by a later run, patched value -> newest wins, no duplicate
    put(lake, "openaq", "measurements", "2026-10-03", "b", "locationid=1__location-1-20240301.csv.gz",
        gzip.compress((hdr + "1,11,BKK,2024-03-01T01:00:00+07:00,13.7,100.5,pm25,µg/m³,21\n").encode()))
    out = staging.stg_air_quality(spark, str(lake))
    assert out["records_in"] == 5 and out["records_duplicate"] == 1
    assert out["quarantine_by_rule"] == {"DQ-06": 1, "DQ-08": 1}
    rows = {(r.sensor_id, r.pollutant): r for r in utc(spark.read.parquet(f"{lake}/staging/air_quality")).collect()}
    assert rows[(11, "PM2.5")].value == 21.0                                   # newest copy kept
    assert rows[(11, "PM2.5")].ts_utc == "2024-02-29 18:00:00"          # 01:00 +07:00 -> UTC
    assert rows[(12, "PM10")].station_id == "openaq:1"
    again = staging.stg_air_quality(spark, str(lake))                         # idempotent re-run
    assert again["records_out"] == out["records_out"] == 2


def test_hotspot_sp_beats_nrt(spark, lake):
    hdr = ("latitude,longitude,bright_ti4,scan,track,acq_date,acq_time,satellite,instrument,"
           "confidence,version,bright_ti5,frp,daynight,type\n")
    row = "18.12345,98.9,330,0.4,0.4,2024-03-01,612,N,VIIRS,{c},2,290,5.5,D,0\n"
    put(lake, "firms", "viirs_area_csv", "2026-10-02", "a", "VIIRS_SNPP_NRT__2024-03-01_5d.csv",
        (hdr + row.format(c="n")).encode())
    put(lake, "firms", "viirs_area_csv", "2026-10-02", "b", "VIIRS_SNPP_SP__2024-03-01_5d.csv",
        (hdr + row.format(c="h") + "17.0,99.0,330,0.4,0.4,2024-03-01,0700,N,VIIRS,x,2,290,1,D,0\n").encode())
    out = staging.stg_hotspot(spark, str(lake))
    assert out["records_in"] == 3 and out["records_duplicate"] == 1 and out["quarantine_by_rule"] == {"DQ-09": 1}
    (r,) = utc(spark.read.parquet(f"{lake}/staging/hotspot")).collect()
    assert r.product == "SP" and r.confidence == "h" and r.ts_utc == "2024-03-01 06:12:00"


def test_weather_explodes_hourly_arrays(spark, lake):
    body = {"latitude": 13.75, "longitude": 100.5, "hourly": {
        "time": ["2024-01-01T00:00", "2024-01-01T01:00"], "temperature_2m": [25.1, 24.8],
        "relative_humidity_2m": [70, 72], "wind_speed_10m": [5.0, 4.0], "wind_direction_10m": [90, 100],
        "precipitation": [0, 0]}}                                               # no boundary_layer_height
    put(lake, "openmeteo", "era5_hourly", "2026-10-02", "a", "cell_137_1005_2024.json", json.dumps(body).encode())
    out = staging.stg_weather(spark, str(lake))
    assert out["records_out"] == 2
    rows = spark.read.parquet(f"{lake}/staging/weather").orderBy("event_ts").collect()
    assert rows[0].point_id == "cell_137_1005" and rows[0].boundary_layer_height is None


def test_gistda_ict_to_utc(spark, lake):
    prov = {"status": 200, "errMsg": "", "data": [
        {"pv_tn": "กรุงเทพมหานคร", "pv_en": "Bangkok", "pv_idn": 10, "pm25": 37.3,
         "dt": "2026-10-02T18:00:00.000Z", "pm25Avg24hr": 32.4}]}
    amph = {"status": 200, "errMsg": "", "data": [
        {"ap_tn": "เมืองเชียงใหม่", "ap_en": "Mueang Chiang Mai", "ap_idn": 5001, "pm25": 12.0,
         "dt": "2026-10-02T18:00:00.000Z", "pm25Avg24hr": 10.0}]}
    put(lake, "gistda", "pm25_amphoe_hourly", "2026-10-02", "a", "province.json", json.dumps(prov).encode())
    put(lake, "gistda", "pm25_amphoe_hourly", "2026-10-02", "a", "amphoe_pv_idn=50.json", json.dumps(amph).encode())
    put(lake, "gistda", "pm25_amphoe_hourly", "2026-10-02", "b", "province.json", json.dumps(prov).encode())
    out = staging.stg_gistda(spark, str(lake))
    assert out["records_out"] == 2 and out["records_duplicate"] == 1
    rows = {r.admin_level: r for r in utc(spark.read.parquet(f"{lake}/staging/gistda_pm25")).collect()}
    assert rows["province"].ts_utc == "2026-10-02 11:00:00"            # 18:00 ICT -> 11:00 UTC
    assert rows["amphoe"].parent_code == 50 and rows["amphoe"].admin_code == 5001


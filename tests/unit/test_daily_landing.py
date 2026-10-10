"""Daily landing functions with fake S3 / HTTP — no network, no Airflow."""

import json
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

from pm25.backfill.sources import Point
from pm25.common.geo import THAILAND_BBOX
from pm25.ingestion.http import HttpResult
from pm25.pipelines import daily_landing as dl

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 2, tzinfo=timezone.utc)


class Lake:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body

    def manifest(self, key):
        return json.loads(self.objects[("lake", key)])


class Body:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


class OpenAQS3:
    def __init__(self, present):
        self.present = set(present)

    def get_object(self, Bucket, Key):
        if Key not in self.present:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": Body(b"gz")}


def test_openaq_day_lands_present_files_and_counts_missing():
    from datetime import date

    from pm25.ingestion import openaq

    present = [openaq.daily_key(1, date(2026, 10, 1)), openaq.daily_key(1, date(2026, 9, 30))]
    lake = Lake()
    out = dl.land_openaq_days(lake=lake, bucket="lake", run_key="r__try1", interval_start=T0, interval_end=T1,
                              location_ids=[1, 2], lookback_days=2, s3=OpenAQS3(present), rate_per_sec=1e6)
    assert out["ok"] == 2 and out["missing_files"] == 4          # 2 stations x 3 days, 2 present
    keys = [k for _, k in lake.objects]
    assert any("raw/source=openaq/dataset=measurements/" in k and "locationid=1__location-1-20261001" in k
               for k in keys)


def firms_csv(n=2):
    rows = "".join(f"18.{i},98.9,330,2026-10-01,0612,N,n,5.0,D\n" for i in range(n))
    return ("latitude,longitude,bright_ti4,acq_date,acq_time,satellite,confidence,frp,daynight\n" + rows).encode()


def test_firms_day_window_and_weight():
    calls = []

    def fetch(url, weight=None, timeout=None):
        calls.append((url, weight))
        return HttpResult(url, 200, firms_csv(), "text/csv")

    lake = Lake()
    out = dl.land_firms_days(lake=lake, bucket="lake", run_key="r__try1", interval_start=T0, interval_end=T1,
                             fetch=fetch, map_key="KEY", sources=["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"],
                             bbox=THAILAND_BBOX)
    assert out["records"] == 4
    assert all(url.endswith("/5/2026-09-27") and w == 5 for url, w in calls)   # D-4 .. D, counted as 5


def test_firms_invalid_key_fails_loudly():
    def fetch(url, weight=None, timeout=None):
        return HttpResult(url, 200, b"Invalid MAP_KEY.", "text/plain")

    with pytest.raises(RuntimeError, match="FIRMS"):
        dl.land_firms_days(lake=Lake(), bucket="lake", run_key="r__try1", interval_start=T0, interval_end=T1,
                           fetch=fetch, map_key="BAD", sources=["VIIRS_SNPP_NRT"], bbox=THAILAND_BBOX)


def test_weather_window_lags_behind_and_all_failed_raises():
    seen = []

    def ok(url, params=None, weight=None, timeout=None):
        seen.append(params)
        return HttpResult(url, 200, json.dumps({"hourly": {"time": ["a"] * 72}}).encode(), "application/json")

    pts = [Point("cell_1_1", 13.7, 100.5), Point("cell_2_2", 18.8, 99.0)]
    out = dl.land_weather_days(lake=Lake(), bucket="lake", run_key="r__try1", interval_start=T0,
                               interval_end=T1, fetch=ok, points=pts)
    assert out["records"] == 144
    assert seen[0]["start_date"] == "2026-09-23" and seen[0]["end_date"] == "2026-09-25"   # lag 6, window 3

    def down(url, params=None, weight=None, timeout=None):
        raise RuntimeError("503")

    with pytest.raises(dl.NothingLanded):
        dl.land_weather_days(lake=Lake(), bucket="lake", run_key="r__try2", interval_start=T0,
                             interval_end=T1, fetch=down, points=pts)

"""Landing logic with a fake object store and fake HTTP — no network, no Docker."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pm25.ingestion.http import HttpResult, RetryableHttpError
from pm25.lake.s3 import ObjectAlreadyExists, put_raw
from pm25.pipelines import gistda_landing as gl

FIX = Path(__file__).resolve().parents[1] / "fixtures"
T0 = datetime(2026, 10, 2, 17, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 10, 2, 18, 0, tzinfo=timezone.utc)


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body


def amphoe_body(pv_idn, n=2):
    return json.dumps({"status": 200, "errMsg": "", "data": [
        {"ap_tn": f"a{i}", "ap_en": f"a{i}", "ap_idn": pv_idn * 100 + i, "pm25": 10.0 + i,
         "dt": "2026-10-02T18:00:00.000Z", "pm25Avg24hr": 9.0} for i in range(n)]}).encode()


def make_fetch(fail_codes=()):
    province = (FIX / "gistda_province.json").read_bytes()

    def fetch(url):
        if url.endswith("getPm25byProvince"):
            return HttpResult(url, 200, province, "application/json")
        code = int(url.rsplit("=", 1)[1])
        if code in fail_codes:
            raise RetryableHttpError(f"HTTP 503 for {url}")
        return HttpResult(url, 200, amphoe_body(code), "application/json")

    return fetch


def land(s3, fetch, try_number=1, **kw):
    return gl.land_hour(client=s3, bucket="lake", run_id="scheduled__2026-10-02T18:20:00+00:00",
                        try_number=try_number, interval_start=T0, interval_end=T1, fetch=fetch, **kw)


def test_land_hour_writes_raw_and_manifest():
    s3 = FakeS3()
    summary = land(s3, make_fetch())
    keys = [k for _, k in s3.objects]
    assert sum(k.endswith(".json") for k in keys) == 1 + 3 + 1   # province + 3 amphoe files + manifest
    assert all(k.startswith("raw/source=gistda/dataset=pm25_amphoe_hourly/") for k in keys)
    assert summary["records"] == 3 + 3 * 2
    manifest = json.loads(s3.objects[("lake", summary["manifest_key"])])
    assert manifest["summary"]["failed"] == 0
    assert all(len(f["sha256"]) == 64 for f in manifest["files"])


def test_retry_writes_new_prefix_never_overwrites():
    s3 = FakeS3()
    first = land(s3, make_fetch(), try_number=1)
    second = land(s3, make_fetch(), try_number=2)
    assert first["raw_prefix"] != second["raw_prefix"]
    assert len(s3.objects) == 10   # nothing overwritten, both attempts kept


def test_raw_is_write_once():
    s3 = FakeS3()
    put_raw(s3, "lake", "raw/x/a.json", b"{}", "application/json")
    with pytest.raises(ObjectAlreadyExists):
        put_raw(s3, "lake", "raw/x/a.json", b"{}", "application/json")
    with pytest.raises(ValueError):
        put_raw(s3, "lake", "staging/x/a.json", b"{}", "application/json")


def test_partial_failure_below_threshold_is_recorded():
    s3 = FakeS3()
    summary = land(s3, make_fetch(fail_codes={74}), max_failed_pct=50)
    assert summary["failed_provinces"] == [74]
    manifest = json.loads(s3.objects[("lake", summary["manifest_key"])])
    issues = gl.check_manifest(manifest, expected_provinces=3, max_failed_pct=50)
    assert not [i for i in issues if i[1] == "FAIL_RUN"]


def test_too_many_failures_raises_after_landing_evidence():
    s3 = FakeS3()
    with pytest.raises(gl.TooManyFailures):
        land(s3, make_fetch(fail_codes={10, 74}), max_failed_pct=10)
    assert any(k.endswith("_manifest.json") for _, k in s3.objects)   # evidence kept for debugging


def test_check_manifest_flags_province_count():
    s3 = FakeS3()
    summary = land(s3, make_fetch())
    manifest = json.loads(s3.objects[("lake", summary["manifest_key"])])
    issues = gl.check_manifest(manifest)          # expects 77, fixture has 3
    assert ("DQ-18", "WARN") in {(r, s) for r, s, _ in issues}


def test_audit_without_dsn_only_logs(monkeypatch):
    monkeypatch.delenv("DWH_DSN", raising=False)
    from pm25 import settings
    settings.get_settings.cache_clear()
    from pm25.audit.run_logger import audited_run

    with audited_run("d", "t", "r", 1, "gistda", T0, T1) as stats:
        stats["records_in"] = 5
    assert stats["pipeline_run_id"] == "d__t__r__try1"

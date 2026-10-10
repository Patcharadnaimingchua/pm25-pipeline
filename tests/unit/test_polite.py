"""The polite client must refuse, pace, back off and log exactly as the policy says."""

import json

import pytest

from pm25.backfill import push, sources
from pm25.ingestion.http import FatalHttpError, RetryableHttpError
from pm25.ingestion.polite import (
    Budget,
    BudgetWaitTooLong,
    CircuitOpen,
    FetchNotAllowed,
    HostPolicy,
    Ledger,
    PoliteClient,
    RollingBudget,
    load_policies,
    retry_after_seconds,
)

UA = "pm25-test/0.1 (student project; contact: team@university.ac.th)"


class Clock:
    def __init__(self):
        self.t = 1000.0
        self.slept = []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


class Resp:
    def __init__(self, status=200, body=b"ok", headers=None, url=""):
        self.status_code, self.content, self.headers, self.url = status, body, headers or {}, url
        self.text = body.decode()


class Session:
    """Scripted responses per URL; robots.txt answered separately."""

    def __init__(self, script=None, robots="User-agent: *\nAllow: /\n"):
        self.script = {k: list(v) for k, v in (script or {}).items()}
        self.robots = robots
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        if url.endswith("/robots.txt"):
            return Resp(200, self.robots.encode(), url=url)
        self.calls.append(url)
        queue = self.script.get(url)
        return queue.pop(0) if queue else Resp(200, b"ok", url=url)


def policy(host="api.test", **kw):
    base = dict(host=host, name=host, rate_per_sec=1000.0, max_concurrency=2)
    base.update(kw)
    return HostPolicy(**base)


def client(tmp_path, policies, session, clock, **kw):
    return PoliteClient({p.host: p for p in policies}, UA, Ledger(tmp_path / "ledger", secrets=("SECRETKEY123",)),
                        session=session, clock=clock, sleep=clock.sleep, **kw)


def ledger_lines(tmp_path):
    return [json.loads(line) for f in sorted((tmp_path / "ledger").glob("*.jsonl"))
            for line in f.read_text().splitlines()]


# ---------------------------------------------------------------- policy file


def test_repo_policy_file_is_valid_and_disables_air4thai():
    policies, contact_required = load_policies()
    assert contact_required is True
    assert not policies["air4thai.pcd.go.th"].enabled
    for p in policies.values():
        if p.robots == "api_terms":
            assert p.robots_reason, p.host
    # budgets stay below published free-tier limits
    om = {b.window_s: b.limit for b in policies["archive-api.open-meteo.com"].budgets}
    assert om[60] < 600 and om[3600] < 5000 and om[86400] < 10000
    assert {b.window_s: b.limit for b in policies["firms.modaps.eosdis.nasa.gov"].budgets}[600] < 5000


# ---------------------------------------------------------------- refusals


def test_refuses_without_contact(tmp_path):
    with pytest.raises(FetchNotAllowed):
        PoliteClient({}, "python-requests/2.32", Ledger(tmp_path))


def test_refuses_unknown_and_disabled_hosts(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy("off.test", rate_per_sec=0, max_concurrency=0)], Session(), clock)
    with pytest.raises(FetchNotAllowed, match="no policy"):
        c.get("https://unknown.test/x")
    with pytest.raises(FetchNotAllowed, match="disabled"):
        c.get("https://off.test/x")


def test_robots_obeyed(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy(robots="obey")], Session(robots="User-agent: *\nDisallow: /\n"), clock)
    with pytest.raises(FetchNotAllowed, match="robots"):
        c.get("https://api.test/data")


def test_api_terms_override_is_recorded_once(tmp_path):
    clock = Clock()
    p = policy(robots="api_terms", robots_reason="documented public API")
    c = client(tmp_path, [p], Session(robots="User-agent: *\nDisallow: /\n"), clock)
    c.get("https://api.test/a")
    c.get("https://api.test/b")
    robots_events = [e for e in ledger_lines(tmp_path) if e["event"] == "robots"]
    assert len(robots_events) == 1 and robots_events[0]["reason"] == "documented public API"


# ---------------------------------------------------------------- back-off


def test_retry_after_is_honoured(tmp_path):
    clock = Clock()
    s = Session({"https://api.test/x": [Resp(429, b"slow down", {"Retry-After": "7"}), Resp(200, b"ok")]})
    res = client(tmp_path, [policy()], s, clock).get("https://api.test/x")
    assert res.body == b"ok"
    assert 7 in clock.slept
    assert [e["status"] for e in ledger_lines(tmp_path) if e["event"] == "request"] == [429, 200]


def test_fatal_4xx_is_not_retried(tmp_path):
    clock = Clock()
    s = Session({"https://api.test/x": [Resp(401, b"bad key")]})
    with pytest.raises(FatalHttpError):
        client(tmp_path, [policy()], s, clock).get("https://api.test/x")
    assert len(s.calls) == 1


def test_circuit_breaker_stops_hammering(tmp_path):
    clock = Clock()
    s = Session({"https://api.test/x": [Resp(503)] * 50})
    c = client(tmp_path, [policy()], s, clock, max_retries=1, breaker_threshold=2)
    for _ in range(2):
        with pytest.raises(RetryableHttpError):
            c.get("https://api.test/x")
    calls_before = len(s.calls)
    with pytest.raises(CircuitOpen):
        c.get("https://api.test/x")
    assert len(s.calls) == calls_before   # no request sent while the circuit is open


# ---------------------------------------------------------------- budgets and pacing


def test_rolling_budget_waits_for_window():
    clock = Clock()
    b = RollingBudget((Budget(3, 60),), clock)
    for _ in range(3):
        assert b.wait_time(1) == 0
        b.record(1)
        clock.t += 1
    assert b.wait_time(1) == pytest.approx(57)   # oldest request expires 60s after it was made


def test_budget_enforced_through_client(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy(budgets=(Budget(2, 60),))], Session(), clock)
    for _ in range(3):
        c.get("https://api.test/x")
    assert max(clock.slept) == pytest.approx(60, abs=0.1)


def test_day_range_weight(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy(budgets=(Budget(10, 600),), weight="day_range")], Session(), clock)
    c.get("https://api.test/x", weight=5)
    c.get("https://api.test/x", weight=5)
    assert not clock.slept or max(clock.slept) < 1
    c.get("https://api.test/x", weight=5)            # 15 > 10 -> must wait for the window
    assert max(clock.slept) == pytest.approx(600, abs=0.1)


def test_pacing(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy(rate_per_sec=0.5)], Session(), clock)
    c.get("https://api.test/x")
    c.get("https://api.test/x")
    assert clock.slept and clock.slept[-1] == pytest.approx(2.0)


def test_budget_seeded_from_previous_runs(tmp_path):
    clock = Clock()
    ledger = Ledger(tmp_path / "ledger")
    for _ in range(2):
        ledger.write(event="request", host="api.test", weight=1, status=200)
    c = PoliteClient({"api.test": policy(budgets=(Budget(2, 3600),))}, UA, ledger, session=Session(),
                     clock=clock, sleep=clock.sleep)
    c.get("https://api.test/x")   # budget already used up by the "earlier process"
    assert max(clock.slept) > 3000


def test_ledger_masks_secrets(tmp_path):
    clock = Clock()
    client(tmp_path, [policy()], Session(), clock).get("https://api.test/csv/SECRETKEY123/VIIRS/1")
    text = "".join(f.read_text() for f in (tmp_path / "ledger").glob("*.jsonl"))
    assert "SECRETKEY123" not in text and "***" in text


def test_retry_after_http_date():
    from datetime import datetime, timezone

    now = datetime(2026, 10, 2, 12, 0, 0, tzinfo=timezone.utc)
    assert retry_after_seconds("Fri, 02 Oct 2026 12:00:30 GMT", now) == pytest.approx(30)
    assert retry_after_seconds("garbage") is None


# ---------------------------------------------------------------- backfill helpers


def test_grid_points_dedupe_same_cell():
    stations = [
        {"location_id": 1, "latitude": "13.7301", "longitude": "100.5201"},   # Bangkok
        {"location_id": 2, "latitude": "13.7499", "longitude": "100.5399"},   # Bangkok, same 0.1 cell
        {"location_id": 3, "latitude": "18.79", "longitude": "98.98"},        # Chiang Mai
    ]
    pts, mapping = sources.grid_points(stations)
    assert len(pts) == 2
    assert mapping["1"] == mapping["2"] != mapping["3"]


def test_openmeteo_archive_resumes(tmp_path):
    class Fake:
        calls = 0

        def get(self, url, params=None, weight=None, timeout=None):
            Fake.calls += 1
            from pm25.ingestion.http import HttpResult
            return HttpResult(url, 200, json.dumps({"hourly": {"time": []}}).encode(), "application/json")

    from datetime import date

    pts = [sources.Point("cell_1_1", 0.1, 0.1)]
    sources.openmeteo_archive(Fake(), pts, date(2022, 1, 1), date(2023, 12, 31), tmp_path)
    sources.openmeteo_archive(Fake(), pts, date(2022, 1, 1), date(2023, 12, 31), tmp_path)
    assert Fake.calls == 2   # 2 years fetched once; the second run downloads nothing


class FakeS3:
    def __init__(self):
        self.objects = {}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = Body


def test_push_landing_is_idempotent_and_dedupes_content(tmp_path):
    landing = tmp_path / "openaq" / "locationid=1"
    landing.mkdir(parents=True)
    (landing / "a.csv.gz").write_bytes(b"same")
    (landing / "b.csv.gz").write_bytes(b"other")
    s3 = FakeS3()
    kw = dict(source="openaq", dataset="measurements", landing_dir=tmp_path / "openaq", pattern="**/*.csv.gz",
              s3=s3, bucket="lake", content_type="application/gzip", workers=2)
    first = push.push_landing(run_id="backfill__1", **kw)
    (landing / "c.csv.gz").write_bytes(b"same")        # identical bytes to a.csv.gz
    second = push.push_landing(run_id="backfill__2", **kw)
    assert first["uploaded"] == 2
    assert second["uploaded"] == 0                     # nothing new: duplicate content is not new data
    assert all(k.startswith("raw/source=openaq/dataset=measurements/") for _, k in s3.objects)


def test_ledger_summary_reports_peaks(tmp_path):
    ledger = Ledger(tmp_path)
    for status in (200, 200, 429):
        ledger.write(event="request", host="api.test", weight=1, status=status, bytes=1000)
    rows = push.ledger_summary(tmp_path)
    assert rows[0]["requests"] == 3 and rows[0]["http_429"] == 1 and rows[0]["peak_per_minute"] == 3


def test_task_mode_fails_fast_instead_of_blocking(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy(budgets=(Budget(2, 3600),))], Session(), clock, max_budget_wait_s=300)
    c.get("https://api.test/x")
    c.get("https://api.test/x")
    with pytest.raises(BudgetWaitTooLong):
        c.get("https://api.test/x")
    assert not clock.slept or max(clock.slept) < 1     # did not sit in a worker slot for an hour
    assert any(e["event"] == "budget_wait_too_long" for e in ledger_lines(tmp_path))


def test_openaq_archive_lists_per_year_and_resumes(tmp_path):
    from datetime import date

    class Body:
        def __init__(self, data):
            self.data = data

        def read(self):
            return self.data

    class Paginator:
        def __init__(self, outer):
            self.outer = outer

        def paginate(self, Bucket, Prefix):
            self.outer.list_calls.append(Prefix)
            keys = [k for k in self.outer.keys if k.startswith(Prefix)]
            yield {"Contents": [{"Key": k, "Size": 3} for k in keys]}

    class S3:
        def __init__(self):
            self.list_calls, self.get_calls = [], []
            self.keys = [
                "records/csv.gz/locationid=1/year=2022/month=01/location-1-20220101.csv.gz",
                "records/csv.gz/locationid=1/year=2022/month=12/location-1-20221231.csv.gz",
                "records/csv.gz/locationid=1/year=2023/month=06/location-1-20230601.csv.gz",
                "records/csv.gz/locationid=1/year=2023/month=06/location-1-20230602.csv.gz",  # outside window
            ]

        def get_paginator(self, name):
            return Paginator(self)

        def get_object(self, Bucket, Key):
            self.get_calls.append(Key)
            return {"Body": Body(b"abc")}

    policy = HostPolicy(host="openaq-data-archive.s3.amazonaws.com", name="s3", rate_per_sec=1000.0,
                        max_concurrency=4)
    s3 = S3()
    window = (date(2022, 1, 1), date(2023, 6, 1))
    out = sources.openaq_archive([1], *window, tmp_path, policy, Ledger(tmp_path / "l"), s3=s3)
    assert len(s3.list_calls) == 2                 # one listing per location-year, not per month
    assert out["files_in_window"] == 3 and out["downloaded"] == 3
    out2 = sources.openaq_archive([1], *window, tmp_path, policy, Ledger(tmp_path / "l"), s3=s3)
    assert out2["downloaded"] == 0 and len(s3.get_calls) == 3   # resumed: nothing fetched twice
    assert not list(tmp_path.rglob("*.part"))


def test_pacer_spaces_requests():
    clock = Clock()
    p = sources.Pacer(2.0, clock=clock, sleep=clock.sleep)
    for _ in range(3):
        p.wait()
    assert clock.slept == [0.5, 0.5]


def test_per_days_weight(tmp_path):
    clock = Clock()
    c = client(tmp_path, [policy(budgets=(Budget(32, 60),), weight="per_days:24")], Session(), clock)
    c.get("https://api.test/x", weight=365)     # one year = 16 calls
    c.get("https://api.test/x", weight=365)     # 32: budget full
    assert not clock.slept or max(clock.slept) < 1
    c.get("https://api.test/x", weight=3)       # a 3-day request = 1 call, but must still wait
    assert max(clock.slept) == pytest.approx(60, abs=0.1)


def test_weather_backfill_skips_failures_and_continues(tmp_path):
    from datetime import date

    from pm25.ingestion.http import HttpResult, RetryableHttpError

    class Flaky:
        n = 0

        def get(self, url, params=None, weight=None, timeout=None):
            Flaky.n += 1
            if Flaky.n == 1:
                raise RetryableHttpError("429 after retries")
            if Flaky.n == 2:
                return HttpResult(url, 200, b"<html>busy</html>", "text/html")   # not JSON
            return HttpResult(url, 200, json.dumps({"hourly": {"time": []}}).encode(), "application/json")

    pts = [sources.Point("a", 1, 1), sources.Point("b", 2, 2)]
    out = sources.openmeteo_archive(Flaky(), pts, date(2022, 1, 1), date(2023, 12, 31), tmp_path)
    assert out["failed"] == 2 and out["new_files"] == 2            # did not crash, kept going
    out2 = sources.openmeteo_archive(Flaky(), pts, date(2022, 1, 1), date(2023, 12, 31), tmp_path)
    assert out2["new_files"] == 2 and out2["failed"] == 0          # re-run fetched only what was missing


def test_weather_backfill_stops_when_circuit_opens(tmp_path):
    from datetime import date

    from pm25.ingestion.polite import CircuitOpen

    class Wall:
        calls = 0

        def get(self, url, params=None, weight=None, timeout=None):
            Wall.calls += 1
            raise CircuitOpen("open")

    pts = [sources.Point("a", 1, 1), sources.Point("b", 2, 2)]
    out = sources.openmeteo_archive(Wall(), pts, date(2022, 1, 1), date(2025, 12, 31), tmp_path)
    assert out["stopped_early"] and Wall.calls == 1      # stopped at once, did not walk all 8 files

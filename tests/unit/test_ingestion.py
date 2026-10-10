import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from pm25.common.geo import THAILAND_BBOX
from pm25.ingestion import firms, gistda, openaq, openmeteo

FIX = Path(__file__).resolve().parents[1] / "fixtures"


# ---------- GISTDA ----------
def test_parse_province_real_payload():
    rows = gistda.parse_province((FIX / "gistda_province.json").read_bytes())
    assert len(rows) == 3
    bkk = next(r for r in rows if r["admin_code"] == 10)
    assert bkk["name_en"] == "Bangkok"
    assert bkk["event_ts"] == datetime(2026, 10, 2, 11, tzinfo=timezone.utc)   # "18:00Z" is 18:00 ICT
    assert bkk["pm25_modeled"] == pytest.approx(37.353, abs=1e-3)
    assert gistda.province_codes(rows) == [10, 11, 74]


def test_parse_24h_real_payload():
    rows = gistda.parse_24h((FIX / "gistda_province_24h.json").read_bytes(), "province", 10)
    assert [r["event_ts"].hour for r in rows] == [11, 12, 13]   # ICT clock times -> UTC
    assert all(r["admin_code"] == 10 for r in rows)


def test_parse_amphoe_shape():
    body = json.dumps({"status": 200, "errMsg": "", "data": [
        {"ap_tn": "เมืองเชียงใหม่", "ap_en": "Mueang Chiang Mai", "ap_idn": 5001,
         "pm25": 12.5, "dt": "2026-10-02T18:00:00.000Z", "pm25Avg24hr": 10.0}]})
    rows = gistda.parse_amphoe(body, 50)
    assert rows[0]["parent_code"] == 50 and rows[0]["admin_level"] == "amphoe"


@pytest.mark.parametrize("body", [
    b"<html>maintenance</html>",
    json.dumps({"status": 500, "errMsg": "db down", "data": []}).encode(),
    json.dumps({"status": 200, "errMsg": "", "data": []}).encode(),
])
def test_gistda_bad_payloads_raise(body):
    with pytest.raises(gistda.GistdaPayloadError):
        gistda.parse_province(body)


def test_gistda_urls():
    assert gistda.amphoe_url(50).endswith("/getPm25byAmphoe?pv_idn=50")
    assert gistda.province_24h_url(10).endswith("/getPM25byProvince24hrs?pv_idn=10")


# ---------- FIRMS ----------
def test_firms_url():
    url = firms.area_url("KEY", "VIIRS_SNPP_SP", THAILAND_BBOX, 5, date(2024, 3, 1))
    assert url == ("https://firms.modaps.eosdis.nasa.gov/api/area/csv/KEY/VIIRS_SNPP_SP/"
                   "97.3,5.5,105.7,20.6/5/2024-03-01")


@pytest.mark.parametrize("kwargs", [
    {"map_key": "", "source": "VIIRS_SNPP_SP", "day_range": 1},
    {"map_key": "K", "source": "NOPE", "day_range": 1},
    {"map_key": "K", "source": "VIIRS_SNPP_SP", "day_range": 6},
])
def test_firms_url_validation(kwargs):
    with pytest.raises(ValueError):
        firms.area_url(bbox=THAILAND_BBOX, **kwargs)


def test_firms_chunks_cover_window_exactly():
    chunks = list(firms.date_chunks(date(2022, 1, 1), date(2025, 12, 31)))
    assert len(chunks) == 293                       # design doc 7.5
    assert sum(n for _, n in chunks) == 1461
    assert all(1 <= n <= 5 for _, n in chunks)


def test_firms_error_text_is_not_csv():
    with pytest.raises(firms.FirmsPayloadError):
        firms.parse_csv(b"Invalid MAP_KEY.")


def test_firms_parse_and_dedup_key():
    body = (b"latitude,longitude,bright_ti4,acq_date,acq_time,satellite,confidence,frp,daynight\n"
            b"18.1,98.9,330.1,2024-03-01,612,N,n,5.2,D\n"
            b"18.1,98.9,330.1,2024-03-01,0612,N,n,5.2,D\n")
    rows = firms.parse_csv(body)
    assert len(rows) == 2
    assert firms.detection_key(rows[0]) == firms.detection_key(rows[1])   # same detection twice


# ---------- OpenAQ ----------
def test_openaq_key_matches_verified_layout():
    assert openaq.daily_key(368400, date(2024, 3, 1)) == (
        "records/csv.gz/locationid=368400/year=2024/month=03/location-368400-20240301.csv.gz")


def test_openaq_summarize_location():
    loc = {"id": 368400, "name": "Bang Kho Laem", "provider": {"id": 1, "name": "Air4Thai"},
           "coordinates": {"latitude": 13.69, "longitude": 100.5},
           "datetimeFirst": {"utc": "2022-10-25T00:00:00Z"}, "datetimeLast": {"utc": "2025-07-01T00:00:00Z"},
           "sensors": [{"parameter": {"name": "pm25"}}]}
    row = openaq.summarize_location(loc)
    assert row["provider"] == "Air4Thai" and row["parameters"] == "pm25"


# ---------- Open-Meteo ----------
def test_openmeteo_params_always_utc():
    p = openmeteo.archive_params(18.79, 98.98, date(2024, 1, 1), date(2024, 12, 31))
    assert p["timezone"] == "UTC"
    assert "boundary_layer_height" in p["hourly"]


# ---------- GISTDA timezone quirk ----------
def test_gistda_dt_is_ict_despite_z_suffix():
    assert gistda.parse_gistda_dt("2026-10-02T18:00:00.000Z") == datetime(2026, 10, 2, 11, tzinfo=timezone.utc)


def test_gistda_dt_plausibility():
    fetched = datetime(2026, 10, 2, 12, 6, tzinfo=timezone.utc)          # 19:06 ICT, observed live
    ok = gistda.parse_gistda_dt("2026-10-02T18:00:00.000Z")             # 11:00 UTC -> 66 min old
    assert gistda.check_dt_plausible(ok, fetched) is None
    as_if_utc = datetime(2026, 10, 2, 18, tzinfo=timezone.utc)          # the naive reading
    assert "future" in gistda.check_dt_plausible(as_if_utc, fetched)
    fixed_api = datetime(2026, 10, 2, 4, tzinfo=timezone.utc)           # if GISTDA starts sending real UTC
    assert "re-verify" in gistda.check_dt_plausible(fixed_api, fetched)

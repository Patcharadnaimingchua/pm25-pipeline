# Data volume verification (Gate G1)

Hard requirement: the final integrated workload must contain **at least 1,000,000 real records**,
counted after deduplication, inside the analysis window **2022-01-01 → 2025-12-31**
(design doc section 7.5). No duplicated, synthetic or padded records count.

The Week-1 scripts append their measured results below automatically, with a UTC timestamp.
Commit this file after each run — it is the evidence used in the final report.

| Step | Command | Needs |
| --- | --- | --- |
| 1 | `make week1-gistda` | internet |
| 2 | `make week1-openaq-locations` | `OPENAQ_API_KEY` |
| 3 | `make week1-openaq-download` | internet (anonymous S3) |
| 4 | `make week1-openaq-count` | `pip install -r requirements-dev.txt` (DuckDB) |
| 5 | `make week1-firms` | `FIRMS_MAP_KEY` |

Gate G1 passes when step 4 reports PM2.5 rows ≥ 1,000,000. If not, follow the fallback order in
design doc 7.5 (more pollutants → more VIIRS satellites → ERA5 at the same stations → GISTDA districts)
and re-count. Never add ERA5 points without a station, or a window without measured PM2.5.

---

# Measured results

## GISTDA admin units — 2026-10-02 12:07 UTC

| Metric | Value |
| --- | --- |
| Provinces returned | 77 |
| Districts returned (sum over provinces) | 928 |
| Provinces failed | 0  |
| District-hour rows per day (= districts x 24) | 22,272 |
| Days to collect 1,000,000 district-hour rows | 44.9 |

## OpenAQ locations in Thailand (API v3, iso=TH) — 2026-10-02 12:07 UTC

| Provider | Locations | Locations with a pm25 sensor |
| --- | --- | --- |
| AirGradient | 346 | 346 |
| Air4Thai | 262 | 262 |
| Thailand | 33 | 0 |
| N/A | 4 | 4 |
| Clarity | 3 | 3 |
| AirNow | 1 | 1 |
| HabitatMap | 1 | 1 |

Total locations: 650

## FIRMS VIIRS_SNPP_SP Thailand bbox 2022-01-01..2025-12-31 — 2026-10-02 12:31 UTC

Bounding box: 97.3,5.5,105.7,20.6 (rectangle: includes some border areas of neighbours)

| Year | Unique detections |
| --- | --- |
| 2022 | 206,687 |
| 2023 | 506,469 |
| 2024 | 415,554 |
| 2025 | 294,707 |

Total unique detections: **1,423,417**

Header seen: `latitude, longitude, bright_ti4, scan, track, acq_date, acq_time, satellite, instrument, confidence, version, bright_ti5, frp, daynight, type`

## OpenAQ archive files 2022-01-01..2025-12-31 (provider ~ 'Air4Thai') — 2026-10-02 13:29 UTC

| Metric | Value |
| --- | --- |
| Locations matching provider | 262 |
| Locations with files on disk | 236 |
| Daily files in window (listed) | 174,810 |
| Newly downloaded this run | 174,810 (70.16 MB) |

## OpenAQ real row count 2022-01-01..2025-12-31 — 2026-10-02 13:56 UTC

Raw rows read: 7,708,409 · after dedup (sensor + timestamp) and window filter: 7,708,037

| Parameter | Rows | Stations | First | Last | Negative values |
| --- | --- | --- | --- | --- | --- |
| pm25 | 3,299,946 | 314 | 2022-01-01 01:00:00+07 | 2025-12-31 23:00:00+07 | 0 |
| pm10 | 1,236,491 | 219 | 2022-01-01 01:00:00+07 | 2025-12-31 23:00:00+07 | 0 |
| o3 | 913,748 | 166 | 2022-01-01 01:00:00+07 | 2025-12-31 23:00:00+07 | 0 |
| no2 | 791,649 | 149 | 2022-01-01 01:00:00+07 | 2025-12-31 23:00:00+07 | 0 |
| so2 | 736,439 | 141 | 2022-01-01 01:00:00+07 | 2025-12-31 23:00:00+07 | 0 |
| co | 729,764 | 139 | 2022-01-01 01:00:00+07 | 2025-12-31 23:00:00+07 | 0 |

**Gate G1 (PM2.5 >= 1,000,000): PASS** — PM2.5 rows = 3,299,946

## Request ledger vs published limits — 2026-10-02 15:28 UTC

| host | requests | weighted | mb | http_429 | errors | peak_per_minute | peak_per_hour | peak_per_day | robots_disallow_events |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| api.openaq.org | 1 | 1 | 0.9 | 0 | 0 | 1 | 1 | 1 | 0 |
| archive-api.open-meteo.com | 683 | 5378 | 233.0 | 113 | 113 | 480 | 4000 | 5378 | 6 |
| firms.modaps.eosdis.nasa.gov | 293 | 1461 | 111.4 | 0 | 0 | 275 | 1461 | 1461 | 0 |
| openaq-data-archive.s3.amazonaws.com | 181475 | 181475 | 70.2 | 0 | 0 | 2928 | 106166 | 181475 | 0 |
| pm25.gistda.or.th | 78 | 78 | 0.2 | 0 | 0 | 75 | 78 | 78 | 0 |

Published limits: config/source_policies.yaml

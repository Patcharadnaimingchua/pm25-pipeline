-- BI-facing views. Power BI reads only mart.* (and audit.*). CREATE OR REPLACE keeps them re-runnable.

CREATE TABLE IF NOT EXISTS mart.pm25_threshold (
  name text PRIMARY KEY, value_ugm3 double precision NOT NULL, averaging text NOT NULL, source text NOT NULL
);
INSERT INTO mart.pm25_threshold VALUES
  ('WHO 2021 guideline', 15.0, '24 h', 'WHO Global Air Quality Guidelines 2021'),
  ('Thailand 24 h standard', 37.5, '24 h', 'Pollution Control Department (PCD); VERIFY the current value before presenting')
ON CONFLICT (name) DO NOTHING;

CREATE OR REPLACE VIEW mart.v_station_day AS
SELECT d.full_date, d.year, d.month, d.month_name, d.season_th, d.is_burning_season, d.is_weekend,
       s.station_id, s.station_name, s.latitude, s.longitude,
       f.pm25_mean, f.pm25_max, f.pm25_hours, f.pm10_mean,
       f.temp_mean, f.rh_mean, f.wind_speed_mean, f.precip_sum, f.blh_mean,
       f.hotspots_50km, f.hotspots_100km, f.hotspots_200km, f.frp_sum_100km, f.frp_sum_200km,
       f.hotspots_high_conf_100km,
       (f.pm25_mean > (SELECT value_ugm3 FROM mart.pm25_threshold WHERE name = 'Thailand 24 h standard')) AS exceeds_th_standard,
       (f.pm25_mean > (SELECT value_ugm3 FROM mart.pm25_threshold WHERE name = 'WHO 2021 guideline')) AS exceeds_who_guideline
FROM dw.fact_air_quality_day f
JOIN dw.dim_date d USING (date_key)
JOIN dw.dim_station s USING (station_key);

-- coverage_pct = valid station-days / (stations seen that month x days in month). The upstream archive has
-- whole months with no data (docs/data_gaps.md), so any monthly mean must be read together with its coverage.
CREATE OR REPLACE VIEW mart.v_monthly_pm25 AS
SELECT year, month, month_name, count(DISTINCT station_id) AS stations,
       avg(pm25_mean) AS pm25_mean, max(pm25_max) AS pm25_max,
       count(*) FILTER (WHERE exceeds_th_standard) AS station_days_over_th_standard,
       count(*) FILTER (WHERE pm25_mean IS NOT NULL) AS valid_station_days,
       round(100.0 * count(*) FILTER (WHERE pm25_mean IS NOT NULL)
             / NULLIF(count(DISTINCT station_id)
                      * extract(day FROM (make_date(year::int, month::int, 1) + INTERVAL '1 month' - INTERVAL '1 day')), 0),
             1) AS coverage_pct,
       (100.0 * count(*) FILTER (WHERE pm25_mean IS NOT NULL)
             / NULLIF(count(DISTINCT station_id)
                      * extract(day FROM (make_date(year::int, month::int, 1) + INTERVAL '1 month' - INTERVAL '1 day')), 0)
        < 50) AS is_low_coverage
FROM mart.v_station_day
GROUP BY year, month, month_name;

CREATE OR REPLACE VIEW mart.v_hotspot_daily AS
SELECT d.full_date, d.year, d.month, count(*) AS hotspots,
       count(*) FILTER (WHERE h.in_thailand_bbox) AS hotspots_in_thailand_bbox,
       count(*) FILTER (WHERE lower(h.confidence) = 'h') AS hotspots_high_conf,
       sum(h.frp) AS frp_sum
FROM dw.fact_hotspot h JOIN dw.dim_date d USING (date_key)
GROUP BY d.full_date, d.year, d.month;

-- Association view for the analysis page (association, not causation): PM2.5 vs hotspots nearby
CREATE OR REPLACE VIEW mart.v_pm25_vs_hotspot AS
SELECT full_date, month, season_th, is_burning_season, station_id, station_name, latitude, longitude,
       pm25_mean, hotspots_100km, frp_sum_100km, hotspots_200km, wind_speed_mean, rh_mean, precip_sum
FROM mart.v_station_day
WHERE pm25_mean IS NOT NULL AND hotspots_100km IS NOT NULL;

CREATE OR REPLACE VIEW mart.v_station_hour_latest AS
SELECT DISTINCT ON (s.station_id) s.station_id, s.station_name, s.latitude, s.longitude,
       f.hour_ts, f.pm25, f.pm25_24h_mean
FROM dw.fact_air_quality_hour f JOIN dw.dim_station s USING (station_key)
WHERE f.pm25 IS NOT NULL
ORDER BY s.station_id, f.hour_ts DESC;

CREATE OR REPLACE VIEW mart.v_modeled_pm25 AS
SELECT a.admin_level, a.admin_code, a.parent_code, a.name_th, a.name_en, f.event_ts, d.full_date,
       f.pm25_modeled, f.pm25_avg_24h
FROM dw.fact_modeled_pm25 f JOIN dw.dim_admin_area a USING (admin_area_key) JOIN dw.dim_date d USING (date_key);

CREATE OR REPLACE VIEW mart.v_row_counts AS
SELECT 'fact_air_quality_hour' AS table_name, count(*) AS n FROM dw.fact_air_quality_hour
UNION ALL SELECT 'fact_air_quality_day', count(*) FROM dw.fact_air_quality_day
UNION ALL SELECT 'fact_hotspot', count(*) FROM dw.fact_hotspot
UNION ALL SELECT 'fact_modeled_pm25', count(*) FROM dw.fact_modeled_pm25;

CREATE OR REPLACE VIEW mart.v_data_quality AS
SELECT r.source_system, r.dag_id, r.task_id, r.started_at, r.status, r.records_in, r.records_out,
       r.records_quarantined, r.records_duplicate
FROM audit.pipeline_run r;

-- How complete is the PM2.5 record per month? Gaps are real gaps in the source archive; they are shown, never filled.
CREATE OR REPLACE VIEW mart.v_pm25_coverage_month AS
WITH rng AS (SELECT min(date_key) AS lo, max(date_key) AS hi FROM dw.fact_air_quality_hour),
months AS (
  SELECT d.year, d.month, count(*) * 24 AS hours_in_month
  FROM dw.dim_date d, rng WHERE d.date_key BETWEEN rng.lo AND rng.hi GROUP BY d.year, d.month),
obs AS (
  SELECT d.year, d.month, count(*) FILTER (WHERE f.pm25 IS NOT NULL) AS pm25_hours,
         count(DISTINCT f.station_key) FILTER (WHERE f.pm25 IS NOT NULL) AS stations_reporting
  FROM dw.fact_air_quality_hour f JOIN dw.dim_date d USING (date_key) GROUP BY d.year, d.month),
total AS (SELECT count(DISTINCT station_key) AS stations_total FROM dw.fact_air_quality_hour WHERE pm25 IS NOT NULL)
SELECT m.year, m.month, COALESCE(o.stations_reporting, 0) AS stations_reporting, t.stations_total,
       COALESCE(o.pm25_hours, 0) AS pm25_hours, m.hours_in_month * t.stations_total AS expected_hours,
       round(100.0 * COALESCE(o.pm25_hours, 0) / NULLIF(m.hours_in_month * t.stations_total, 0), 1) AS coverage_pct
FROM months m CROSS JOIN total t LEFT JOIN obs o USING (year, month);

-- Demo queries (read-only). In DBeaver: Option+X runs all, Cmd+Enter runs the statement at the cursor.
-- Connection: localhost:5433, database pm25, user dwh.

-- A. How much data -------------------------------------------------------------
-- 1) Rows per fact table
select 'fact_air_quality_hour' as t, count(*) n from dw.fact_air_quality_hour
union all select 'fact_air_quality_day', count(*) from dw.fact_air_quality_day
union all select 'fact_hotspot', count(*) from dw.fact_hotspot
union all select 'fact_modeled_pm25', count(*) from dw.fact_modeled_pm25;

-- 2) Real PM2.5 rows, stations, time range
select count(*) rows_all, count(pm25) rows_with_pm25,
       count(distinct station_key) stations,
       min(hour_ts) first_ts, max(hour_ts) last_ts
from dw.fact_air_quality_hour;

-- 3) PM2.5 rows per year (Thai time)
select extract(year from hour_ts at time zone 'Asia/Bangkok') as yr, count(pm25) pm25_rows
from dw.fact_air_quality_hour group by 1 order by 1;

-- 4) Tables per schema
select table_schema, count(*) tables
from information_schema.tables
where table_schema in ('stg','dw','mart','audit') group by 1 order by 1;

-- B. What can be analysed ------------------------------------------------------
-- 5) Monthly mean PM2.5
select year, month, pm25_mean, pm25_max, stations from mart.v_monthly_pm25 order by year, month;

-- 6) Five highest months
select year, month_name, pm25_mean from mart.v_monthly_pm25
where pm25_mean is not null order by pm25_mean desc limit 5;

-- 7) Share of station-days above the Thai 24 h standard (37.5)
select count(*) station_days,
       count(*) filter (where exceeds_th_standard) over_thai,
       round(100.0 * count(*) filter (where exceeds_th_standard) / count(*), 1) pct_over
from mart.v_station_day;

-- 8) Ten stations with the highest mean PM2.5
select station_id, round(avg(pm25_mean)::numeric,1) avg_pm25, count(*) days
from mart.v_station_day group by station_id order by avg_pm25 desc limit 10;

-- 9) Hotspots per year
select extract(year from full_date) yr, sum(hotspots) hotspots
from mart.v_hotspot_daily group by 1 order by 1;

-- C. Quality and trust ---------------------------------------------------------
-- 10) Months with the lowest data coverage
select * from mart.v_pm25_coverage_month order by coverage_pct limit 10;

-- 11) Pipeline task results
select task_id, records_in, records_out, records_duplicate, records_quarantined, status
from mart.v_data_quality;

-- 12) Lake vs warehouse (latest load)
select table_name, rows_in_lake, rows_in_dw, matches
from audit.load_reconciliation order by load_id desc limit 4;

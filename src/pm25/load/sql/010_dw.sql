-- Star schema (design doc section 18). Idempotent: safe to run on every load.
-- Surrogate keys are generated here; natural keys are unique so reloads never duplicate rows.

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------- dimensions

CREATE TABLE IF NOT EXISTS dw.dim_date (
  date_key          int PRIMARY KEY,               -- yyyymmdd (Asia/Bangkok calendar day)
  full_date         date NOT NULL UNIQUE,
  year              smallint NOT NULL,
  quarter           smallint NOT NULL,
  month             smallint NOT NULL,
  month_name        text NOT NULL,
  day_of_month      smallint NOT NULL,
  day_of_week       smallint NOT NULL,             -- 1 = Monday .. 7 = Sunday (ISO)
  day_name          text NOT NULL,
  is_weekend        boolean NOT NULL,
  season_th         text NOT NULL,                 -- Thai seasons: hot Mar-May, rainy Jun-Oct, cool Nov-Feb
  is_burning_season boolean NOT NULL               -- Jan-Apr: the usual northern/regional smoke period
);

INSERT INTO dw.dim_date
SELECT to_char(d, 'YYYYMMDD')::int, d,
       extract(year FROM d)::smallint, extract(quarter FROM d)::smallint, extract(month FROM d)::smallint,
       to_char(d, 'FMMonth'), extract(day FROM d)::smallint, extract(isodow FROM d)::smallint,
       to_char(d, 'FMDay'), extract(isodow FROM d) >= 6,
       CASE WHEN extract(month FROM d) IN (3, 4, 5) THEN 'hot'
            WHEN extract(month FROM d) BETWEEN 6 AND 10 THEN 'rainy' ELSE 'cool' END,
       extract(month FROM d) BETWEEN 1 AND 4
FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

CREATE TABLE IF NOT EXISTS dw.dim_station (
  station_key   serial PRIMARY KEY,
  station_id    text NOT NULL UNIQUE,              -- natural key, e.g. openaq:1172475
  location_id   bigint NOT NULL,
  station_name  text,
  latitude      double precision NOT NULL,
  longitude     double precision NOT NULL,
  provider      text NOT NULL DEFAULT 'Air4Thai via OpenAQ',
  weather_point_id text,                           -- nearest ERA5 0.1-degree cell
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw.dim_weather_point (
  weather_point_key serial PRIMARY KEY,
  point_id          text NOT NULL UNIQUE,
  grid_latitude     double precision NOT NULL,
  grid_longitude    double precision NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_admin_area (
  admin_area_key serial PRIMARY KEY,
  admin_level    text NOT NULL CHECK (admin_level IN ('province', 'amphoe')),
  admin_code     int  NOT NULL,
  parent_code    int,
  name_th        text,
  name_en        text,
  UNIQUE (admin_level, admin_code)
);

-- ---------------------------------------------------------------- facts

CREATE TABLE IF NOT EXISTS dw.fact_air_quality_hour (
  station_key      int NOT NULL REFERENCES dw.dim_station (station_key),
  hour_ts          timestamptz NOT NULL,           -- UTC instant at the start of the hour
  date_key         int NOT NULL REFERENCES dw.dim_date (date_key),
  hour_of_day      smallint NOT NULL,              -- Asia/Bangkok local hour 0-23
  pm25             double precision,
  pm10             double precision,
  o3               double precision,
  no2              double precision,
  so2              double precision,
  co               double precision,
  pm25_24h_mean    double precision,               -- NULL unless >= 18 of the last 24 hours exist
  pm25_24h_n       smallint,
  temperature_2m   double precision,
  relative_humidity_2m double precision,
  wind_speed_10m   double precision,
  wind_direction_10m double precision,
  precipitation    double precision,
  boundary_layer_height double precision,
  PRIMARY KEY (station_key, hour_ts)
);
CREATE INDEX IF NOT EXISTS fact_aq_hour_date_idx ON dw.fact_air_quality_hour (date_key);

CREATE TABLE IF NOT EXISTS dw.fact_air_quality_day (
  station_key      int NOT NULL REFERENCES dw.dim_station (station_key),
  date_key         int NOT NULL REFERENCES dw.dim_date (date_key),
  pm25_mean        double precision,               -- NULL unless >= 18 valid hours
  pm25_max         double precision,
  pm25_hours       smallint NOT NULL,
  pm10_mean        double precision,
  temp_mean        double precision,
  rh_mean          double precision,
  wind_speed_mean  double precision,
  precip_sum       double precision,
  blh_mean         double precision,
  weather_hours    smallint,
  hotspots_50km    int,                            -- NULL = outside FIRMS coverage (not a measured zero)
  hotspots_100km   int,
  hotspots_200km   int,
  frp_sum_50km     double precision,
  frp_sum_100km    double precision,
  frp_sum_200km    double precision,
  hotspots_high_conf_100km int,
  PRIMARY KEY (station_key, date_key)
);
CREATE INDEX IF NOT EXISTS fact_aq_day_date_idx ON dw.fact_air_quality_day (date_key);

CREATE TABLE IF NOT EXISTS dw.fact_hotspot (
  hotspot_id     text PRIMARY KEY,                 -- sha1 of satellite + rounded position + acquisition time
  date_key       int NOT NULL REFERENCES dw.dim_date (date_key),
  event_ts       timestamptz NOT NULL,
  latitude       double precision NOT NULL,
  longitude      double precision NOT NULL,
  satellite      text,
  instrument     text,
  product        text,
  confidence     text,
  frp            double precision,
  bright_ti4     double precision,
  daynight       text,
  in_thailand_bbox boolean NOT NULL
);
CREATE INDEX IF NOT EXISTS fact_hotspot_date_idx ON dw.fact_hotspot (date_key);

CREATE TABLE IF NOT EXISTS dw.fact_modeled_pm25 (
  admin_area_key int NOT NULL REFERENCES dw.dim_admin_area (admin_area_key),
  event_ts       timestamptz NOT NULL,
  date_key       int NOT NULL REFERENCES dw.dim_date (date_key),
  pm25_modeled   double precision NOT NULL,
  pm25_avg_24h   double precision,
  PRIMARY KEY (admin_area_key, event_ts)
);

-- ---------------------------------------------------------------- load bookkeeping

CREATE TABLE IF NOT EXISTS audit.load_reconciliation (
  load_id        bigserial PRIMARY KEY,
  loaded_at      timestamptz NOT NULL DEFAULT now(),
  table_name     text NOT NULL,
  rows_in_lake   bigint NOT NULL,
  rows_in_dw     bigint NOT NULL,
  matches        boolean NOT NULL
);

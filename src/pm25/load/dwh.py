"""Load curated/staging Parquet into the PostgreSQL star schema (design doc sections 18-19).

Flow: Spark writes each table as CSV to a local work folder -> psycopg COPY into a TEMP table ->
one INSERT ... ON CONFLICT DO UPDATE per target inside a single transaction. The natural keys are
unique, so re-running the load with the same lake gives the same warehouse (idempotent), and a failed
run rolls back completely. After loading, row counts lake vs warehouse are compared and stored in
audit.load_reconciliation.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pm25.spark.curated import LOCAL_TZ, RADII_KM, station_table, weather_point_id

log = logging.getLogger(__name__)
SQL_DIR = Path(__file__).parent / "sql"
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"      # session time zone is UTC, so this is the UTC instant


# ------------------------------------------------------------------ what goes to Postgres

def _local_date_cols(df: DataFrame, ts_col: str) -> DataFrame:
    local = F.from_utc_timestamp(ts_col, LOCAL_TZ)
    return df.withColumn("local_date", F.to_date(local)).withColumn("hour_of_day", F.hour(local))


def build_exports(spark: SparkSession, lake: str) -> dict[str, DataFrame]:
    """name -> DataFrame whose column order matches the TEMP table of the same name below."""
    stations = (station_table(spark, lake)
                .withColumn("weather_point_id", weather_point_id(F.col("latitude"), F.col("longitude")))
                .select("station_id", "location_id", F.col("location_name").alias("station_name"),
                        "latitude", "longitude", "weather_point_id"))
    weather_points = (spark.read.parquet(f"{lake}/staging/weather")
                      .groupBy("point_id").agg(F.first("grid_latitude").alias("grid_latitude"),
                                               F.first("grid_longitude").alias("grid_longitude")))
    gistda = spark.read.parquet(f"{lake}/staging/gistda_pm25")
    admin = (gistda.groupBy("admin_level", "admin_code")
             .agg(F.max("parent_code").alias("parent_code"), F.max("name_th").alias("name_th"),
                  F.max("name_en").alias("name_en")))

    hour = _local_date_cols(spark.read.parquet(f"{lake}/curated/station_hour"), "hour_ts")
    hour = hour.select("station_id", "hour_ts", "local_date", "hour_of_day", "pm25", "pm10", "o3", "no2", "so2",
                       "co", "pm25_24h_mean", F.col("pm25_24h_n").cast("int").alias("pm25_24h_n"),
                       "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
                       "precipitation", "boundary_layer_height")

    day = spark.read.parquet(f"{lake}/curated/station_day")
    hs = spark.read.parquet(f"{lake}/curated/hotspot_station_day")
    hs_cols = [f"hotspots_{r}km" for r in RADII_KM] + [f"frp_sum_{r}km" for r in RADII_KM] + [
        "hotspots_high_conf_100km"]
    day = (day.join(hs.select("station_id", "local_date", *hs_cols), ["station_id", "local_date"], "left")
           .select("station_id", "local_date", "pm25_mean", "pm25_max",
                   F.col("pm25_hours").cast("int").alias("pm25_hours"),
                   "pm10_mean", "temp_mean", "rh_mean", "wind_speed_mean", "precip_sum", "blh_mean",
                   F.col("weather_hours").cast("int").alias("weather_hours"),
                   "hotspots_50km", "hotspots_100km", "hotspots_200km", "frp_sum_50km", "frp_sum_100km",
                   "frp_sum_200km", "hotspots_high_conf_100km"))

    hotspot = (spark.read.parquet(f"{lake}/staging/hotspot")
               .withColumn("local_date", F.to_date(F.from_utc_timestamp("event_ts", LOCAL_TZ)))
               .select("hotspot_id", "local_date", "event_ts", "latitude", "longitude", "satellite", "instrument",
                       "product", "confidence", "frp", "bright_ti4", "daynight", "in_thailand_bbox"))

    modeled = (gistda.withColumn("local_date", F.to_date(F.from_utc_timestamp("event_ts", LOCAL_TZ)))
               .select("admin_level", "admin_code", "event_ts", "local_date", "pm25_modeled", "pm25_avg_24h"))
    return {"station": stations, "weather_point": weather_points, "admin_area": admin, "air_hour": hour,
            "air_day": day, "hotspot": hotspot, "modeled": modeled}


# TEMP tables (column order = export order) -------------------------------------------------

TEMP_DDL = {
    "station": "station_id text, location_id bigint, station_name text, latitude float8, longitude float8, "
               "weather_point_id text",
    "weather_point": "point_id text, grid_latitude float8, grid_longitude float8",
    "admin_area": "admin_level text, admin_code int, parent_code int, name_th text, name_en text",
    "air_hour": "station_id text, hour_ts timestamptz, local_date date, hour_of_day int, pm25 float8, pm10 float8, "
                "o3 float8, no2 float8, so2 float8, co float8, pm25_24h_mean float8, pm25_24h_n int, "
                "temperature_2m float8, relative_humidity_2m float8, wind_speed_10m float8, "
                "wind_direction_10m float8, precipitation float8, boundary_layer_height float8",
    "air_day": "station_id text, local_date date, pm25_mean float8, pm25_max float8, pm25_hours int, pm10_mean float8, "
               "temp_mean float8, rh_mean float8, wind_speed_mean float8, precip_sum float8, blh_mean float8, "
               "weather_hours int, hotspots_50km int, hotspots_100km int, hotspots_200km int, frp_sum_50km float8, "
               "frp_sum_100km float8, frp_sum_200km float8, hotspots_high_conf_100km int",
    "hotspot": "hotspot_id text, local_date date, event_ts timestamptz, latitude float8, longitude float8, "
               "satellite text, instrument text, product text, confidence text, frp float8, bright_ti4 float8, "
               "daynight text, in_thailand_bbox boolean",
    "modeled": "admin_level text, admin_code int, event_ts timestamptz, local_date date, pm25_modeled float8, "
               "pm25_avg_24h float8",
}

DATE_KEY = "to_char(t.local_date, 'YYYYMMDD')::int"

UPSERTS = {          # order matters: dimensions first
    "station": """
        INSERT INTO dw.dim_station (station_id, location_id, station_name, latitude, longitude, weather_point_id)
        SELECT station_id, location_id, station_name, latitude, longitude, weather_point_id FROM t_station
        ON CONFLICT (station_id) DO UPDATE SET location_id = EXCLUDED.location_id,
          station_name = EXCLUDED.station_name, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
          weather_point_id = EXCLUDED.weather_point_id, updated_at = now()""",
    "weather_point": """
        INSERT INTO dw.dim_weather_point (point_id, grid_latitude, grid_longitude)
        SELECT point_id, grid_latitude, grid_longitude FROM t_weather_point
        ON CONFLICT (point_id) DO UPDATE SET grid_latitude = EXCLUDED.grid_latitude,
          grid_longitude = EXCLUDED.grid_longitude""",
    "admin_area": """
        INSERT INTO dw.dim_admin_area (admin_level, admin_code, parent_code, name_th, name_en)
        SELECT admin_level, admin_code, parent_code, name_th, name_en FROM t_admin_area
        ON CONFLICT (admin_level, admin_code) DO UPDATE SET parent_code = EXCLUDED.parent_code,
          name_th = EXCLUDED.name_th, name_en = EXCLUDED.name_en""",
    "air_hour": f"""
        INSERT INTO dw.fact_air_quality_hour (station_key, hour_ts, date_key, hour_of_day, pm25, pm10, o3, no2, so2, co,
          pm25_24h_mean, pm25_24h_n, temperature_2m, relative_humidity_2m, wind_speed_10m, wind_direction_10m,
          precipitation, boundary_layer_height)
        SELECT s.station_key, t.hour_ts, {DATE_KEY}, t.hour_of_day, t.pm25, t.pm10, t.o3, t.no2, t.so2, t.co,
          t.pm25_24h_mean, t.pm25_24h_n, t.temperature_2m, t.relative_humidity_2m, t.wind_speed_10m,
          t.wind_direction_10m, t.precipitation, t.boundary_layer_height
        FROM t_air_hour t JOIN dw.dim_station s USING (station_id)
        ON CONFLICT (station_key, hour_ts) DO UPDATE SET date_key = EXCLUDED.date_key,
          hour_of_day = EXCLUDED.hour_of_day, pm25 = EXCLUDED.pm25, pm10 = EXCLUDED.pm10, o3 = EXCLUDED.o3,
          no2 = EXCLUDED.no2, so2 = EXCLUDED.so2, co = EXCLUDED.co, pm25_24h_mean = EXCLUDED.pm25_24h_mean,
          pm25_24h_n = EXCLUDED.pm25_24h_n, temperature_2m = EXCLUDED.temperature_2m,
          relative_humidity_2m = EXCLUDED.relative_humidity_2m, wind_speed_10m = EXCLUDED.wind_speed_10m,
          wind_direction_10m = EXCLUDED.wind_direction_10m, precipitation = EXCLUDED.precipitation,
          boundary_layer_height = EXCLUDED.boundary_layer_height""",
    "air_day": f"""
        INSERT INTO dw.fact_air_quality_day (station_key, date_key, pm25_mean, pm25_max, pm25_hours, pm10_mean,
          temp_mean, rh_mean, wind_speed_mean, precip_sum, blh_mean, weather_hours, hotspots_50km, hotspots_100km,
          hotspots_200km, frp_sum_50km, frp_sum_100km, frp_sum_200km, hotspots_high_conf_100km)
        SELECT s.station_key, {DATE_KEY}, t.pm25_mean, t.pm25_max, t.pm25_hours, t.pm10_mean, t.temp_mean, t.rh_mean,
          t.wind_speed_mean, t.precip_sum, t.blh_mean, t.weather_hours, t.hotspots_50km, t.hotspots_100km,
          t.hotspots_200km, t.frp_sum_50km, t.frp_sum_100km, t.frp_sum_200km, t.hotspots_high_conf_100km
        FROM t_air_day t JOIN dw.dim_station s USING (station_id)
        ON CONFLICT (station_key, date_key) DO UPDATE SET pm25_mean = EXCLUDED.pm25_mean,
          pm25_max = EXCLUDED.pm25_max, pm25_hours = EXCLUDED.pm25_hours, pm10_mean = EXCLUDED.pm10_mean,
          temp_mean = EXCLUDED.temp_mean, rh_mean = EXCLUDED.rh_mean, wind_speed_mean = EXCLUDED.wind_speed_mean,
          precip_sum = EXCLUDED.precip_sum, blh_mean = EXCLUDED.blh_mean, weather_hours = EXCLUDED.weather_hours,
          hotspots_50km = EXCLUDED.hotspots_50km, hotspots_100km = EXCLUDED.hotspots_100km,
          hotspots_200km = EXCLUDED.hotspots_200km, frp_sum_50km = EXCLUDED.frp_sum_50km,
          frp_sum_100km = EXCLUDED.frp_sum_100km, frp_sum_200km = EXCLUDED.frp_sum_200km,
          hotspots_high_conf_100km = EXCLUDED.hotspots_high_conf_100km""",
    "hotspot": f"""
        INSERT INTO dw.fact_hotspot (hotspot_id, date_key, event_ts, latitude, longitude, satellite, instrument,
          product, confidence, frp, bright_ti4, daynight, in_thailand_bbox)
        SELECT t.hotspot_id, {DATE_KEY}, t.event_ts, t.latitude, t.longitude, t.satellite, t.instrument, t.product,
          t.confidence, t.frp, t.bright_ti4, t.daynight, t.in_thailand_bbox FROM t_hotspot t
        ON CONFLICT (hotspot_id) DO UPDATE SET date_key = EXCLUDED.date_key, event_ts = EXCLUDED.event_ts,
          latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude, satellite = EXCLUDED.satellite,
          instrument = EXCLUDED.instrument, product = EXCLUDED.product, confidence = EXCLUDED.confidence,
          frp = EXCLUDED.frp, bright_ti4 = EXCLUDED.bright_ti4, daynight = EXCLUDED.daynight,
          in_thailand_bbox = EXCLUDED.in_thailand_bbox""",
    "modeled": f"""
        INSERT INTO dw.fact_modeled_pm25 (admin_area_key, event_ts, date_key, pm25_modeled, pm25_avg_24h)
        SELECT a.admin_area_key, t.event_ts, {DATE_KEY}, t.pm25_modeled, t.pm25_avg_24h
        FROM t_modeled t JOIN dw.dim_admin_area a USING (admin_level, admin_code)
        ON CONFLICT (admin_area_key, event_ts) DO UPDATE SET date_key = EXCLUDED.date_key,
          pm25_modeled = EXCLUDED.pm25_modeled, pm25_avg_24h = EXCLUDED.pm25_avg_24h""",
}

TARGET_TABLE = {"air_hour": "dw.fact_air_quality_hour", "air_day": "dw.fact_air_quality_day",
                "hotspot": "dw.fact_hotspot", "modeled": "dw.fact_modeled_pm25"}


# ------------------------------------------------------------------ steps

def apply_ddl(conn) -> None:
    """Audit tables first (the mart views read them), then the star schema, then the views."""
    for f in sorted(SQL_DIR.glob("*.sql")):
        conn.execute(f.read_text(encoding="utf-8"))
        log.info("applied %s", f.name)


def export_csv(df: DataFrame, out_dir: Path, parts: int = 4) -> list[Path]:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    (df.coalesce(parts).write.mode("overwrite")
       .option("header", "false").option("escape", '"').option("quote", '"')
       .option("timestampFormat", TS_FORMAT).option("nullValue", "").option("emptyValue", '""')
       .csv(f"file://{out_dir}"))
    return sorted(out_dir.glob("part-*.csv"))


def copy_files(conn, table: str, files: list[Path]) -> None:
    with conn.cursor() as cur, cur.copy(f"COPY {table} FROM STDIN WITH (FORMAT csv)") as cp:
        for f in files:
            with f.open("rb") as fh:
                while chunk := fh.read(1 << 20):
                    cp.write(chunk)


def run_load(spark: SparkSession, lake: str, dsn: str, workdir: str = "/tmp/pm25_load") -> dict:
    import psycopg

    exports = build_exports(spark, lake)
    work = Path(workdir)
    files = {name: export_csv(df, work / name) for name, df in exports.items()}
    lake_rows = {name: df.count() for name, df in exports.items()}       # counted from the same frames

    result: dict = {"lake_rows": lake_rows, "dw_rows": {}, "reconciled": {}}
    with psycopg.connect(dsn) as conn:                                    # one transaction: all or nothing
        apply_ddl(conn)
        for name in UPSERTS:
            conn.execute(f"CREATE TEMP TABLE t_{name} ({TEMP_DDL[name]}) ON COMMIT DROP")
            copy_files(conn, f"t_{name}", files[name])
            conn.execute(UPSERTS[name])
            log.info("loaded %s", name)
        for name, table in TARGET_TABLE.items():
            n = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            result["dw_rows"][name] = n
            ok = n == lake_rows[name]
            result["reconciled"][name] = ok
            conn.execute("INSERT INTO audit.load_reconciliation (table_name, rows_in_lake, rows_in_dw, matches) "
                         "VALUES (%s, %s, %s, %s)", (table, lake_rows[name], n, ok))
    shutil.rmtree(work, ignore_errors=True)
    return result

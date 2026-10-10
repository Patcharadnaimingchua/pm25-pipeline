"""Staging jobs: raw (as received) -> staging (typed, UTC, deduplicated Parquet) + quarantine.

Each job rebuilds its staging dataset from everything in raw (idempotent: the same raw gives the same
staging, re-runs never duplicate) and returns counts for the audit tables.

DQ rules applied here (design doc section 15): DQ-02 timestamp missing, DQ-03 value missing,
DQ-06 negative concentration, DQ-07 PM2.5 > 1000, DQ-08 coordinates outside Thailand,
DQ-09 invalid FIRMS confidence, DQ-11/12 uniqueness (enforced by dedup, checked after).
"""

from __future__ import annotations

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pm25.common.geo import THAILAND_BBOX as BB

POLLUTANT_NAMES = {"pm25": "PM2.5", "pm10": "PM10", "o3": "O3", "no2": "NO2", "so2": "SO2", "co": "CO",
                   "pm1": "PM1"}


def _read_raw_text(spark: SparkSession, folder: str, glob: str, fmt: str, **options) -> DataFrame:
    reader = (spark.read.format(fmt).option("recursiveFileLookup", "true").option("pathGlobFilter", glob))
    for k, v in options.items():
        reader = reader.option(k, v)
    return reader.load(folder).withColumn("source_file", F.col("_metadata.file_path"))


def _ingest_date(col="source_file"):
    return F.to_date(F.regexp_extract(F.col(col), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1))


def _latest_per_key(df: DataFrame, keys: list[str], order: list) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _first_failed_rule(*rules):
    """rules: (rule_id, condition). Returns the first rule a row fails, or null."""
    expr = F.lit(None).cast("string")
    for rule_id, cond in reversed(rules):
        expr = F.when(cond, F.lit(rule_id)).otherwise(expr)
    return expr


def _write(df_valid: DataFrame, df_bad: DataFrame, lake: str, dataset: str, partition: str) -> dict:
    (df_valid.repartition(partition).sortWithinPartitions(partition)
     .write.mode("overwrite").partitionBy(partition).parquet(f"{lake}/staging/{dataset}"))
    df_bad.write.mode("overwrite").parquet(f"{lake}/quarantine/{dataset}")
    spark = df_valid.sparkSession
    valid = spark.read.parquet(f"{lake}/staging/{dataset}")
    bad = spark.read.parquet(f"{lake}/quarantine/{dataset}")
    by_rule = {r["dq_rule"]: r["n"] for r in bad.groupBy("dq_rule").agg(F.count("*").alias("n")).collect()}
    return {"dataset": dataset, "records_out": valid.count(), "records_quarantined": sum(by_rule.values()),
            "quarantine_by_rule": by_rule}


def _in_thailand(lat, lon):
    return lat.between(BB.south, BB.north) & lon.between(BB.west, BB.east)


# ------------------------------------------------------------------ OpenAQ / Air4Thai


def stg_air_quality(spark: SparkSession, lake: str) -> dict:
    raw = _read_raw_text(spark, f"{lake}/raw/source=openaq/dataset=measurements", "*.csv.gz", "csv",
                         header="true", inferSchema="false")
    df = (raw.select(
        F.col("location_id").cast("long").alias("location_id"),
        F.col("sensors_id").cast("long").alias("sensor_id"),
        F.col("location").alias("location_name"),
        F.to_timestamp("datetime").alias("event_ts"),            # ISO with offset -> UTC instant
        F.col("lat").cast("double").alias("latitude"),
        F.col("lon").cast("double").alias("longitude"),
        F.lower("parameter").alias("parameter"),
        F.col("units").alias("unit"),
        F.col("value").cast("double").alias("value"),
        "source_file")
        .withColumn("pollutant", F.coalesce(F.element_at(F.create_map(
            *[x for kv in POLLUTANT_NAMES.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]), F.col("parameter")),
            F.upper("parameter")))
        .withColumn("station_id", F.concat(F.lit("openaq:"), F.col("location_id")))
        .withColumn("ingest_date", _ingest_date()))
    # ~175k tiny objects: read them from the lake ONCE (cached), not once per count/write pass
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    records_in = df.count()
    # newest ingested copy wins (archive files may be patched retroactively)
    df = _latest_per_key(df, ["sensor_id", "event_ts"], [F.col("ingest_date").desc(), F.col("source_file").desc()])
    rule = _first_failed_rule(
        ("DQ-02", F.col("event_ts").isNull()),
        ("DQ-03", F.col("value").isNull()),
        ("DQ-06", F.col("value") < 0),
        ("DQ-07", (F.col("pollutant") == "PM2.5") & (F.col("value") > 1000)),
        ("DQ-08", ~_in_thailand(F.col("latitude"), F.col("longitude"))),
    )
    df = df.withColumn("dq_rule", rule).withColumn("event_date", F.to_date("event_ts"))
    out = _write(df.filter("dq_rule IS NULL").drop("dq_rule", "parameter"),
                 df.filter("dq_rule IS NOT NULL"), lake, "air_quality", "event_date")
    out["records_in"] = records_in
    out["records_duplicate"] = records_in - df.count()
    df.unpersist()
    return out


# ------------------------------------------------------------------ NASA FIRMS


def stg_hotspot(spark: SparkSession, lake: str) -> dict:
    raw = _read_raw_text(spark, f"{lake}/raw/source=firms", "*.csv", "csv", header="true", inferSchema="false")
    records_in = raw.count()
    fname = F.element_at(F.split("source_file", "/"), -1)
    df = (raw
          .withColumn("source_product", F.regexp_extract(fname, r"^(VIIRS_[A-Z0-9]+_(?:SP|NRT))", 1))
          .withColumn("product", F.regexp_extract("source_product", r"_(SP|NRT)$", 1))
          .withColumn("latitude", F.col("latitude").cast("double"))
          .withColumn("longitude", F.col("longitude").cast("double"))
          .withColumn("acq_time4", F.lpad(F.col("acq_time").cast("string"), 4, "0"))
          .withColumn("event_ts", F.to_timestamp(F.concat_ws(" ", "acq_date", "acq_time4"), "yyyy-MM-dd HHmm"))
          .withColumn("frp", F.col("frp").cast("double"))
          .withColumn("bright_ti4", F.col("bright_ti4").cast("double"))
          .withColumn("ingest_date", _ingest_date()))
    key = ["satellite", "lat_r", "lon_r", "acq_date", "acq_time4"]
    df = (df.withColumn("lat_r", F.round("latitude", 4)).withColumn("lon_r", F.round("longitude", 4)))
    # science-quality (SP) beats near-real-time (NRT) for the same detection; then the newest file
    df = _latest_per_key(df, key, [F.when(F.col("product") == "SP", 0).otherwise(1),
                                   F.col("ingest_date").desc(), F.col("source_file").desc()])
    df = (df.withColumn("hotspot_id", F.sha1(F.concat_ws("|", *key)))
          .withColumn("in_thailand_bbox", _in_thailand(F.col("latitude"), F.col("longitude"))))
    rule = _first_failed_rule(
        ("DQ-02", F.col("event_ts").isNull()),
        ("DQ-08", F.col("latitude").isNull() | F.col("longitude").isNull()),
        ("DQ-09", ~F.lower("confidence").isin("l", "n", "h")),
    )
    df = df.withColumn("dq_rule", rule).withColumn("event_date", F.to_date("event_ts"))
    cols = ["hotspot_id", "satellite", "instrument", "product", "latitude", "longitude", "event_ts", "event_date",
            "confidence", "frp", "bright_ti4", "daynight", "in_thailand_bbox", "source_file", "ingest_date"]
    out = _write(df.filter("dq_rule IS NULL").select(*cols),
                 df.filter("dq_rule IS NOT NULL").select(*cols, "dq_rule"), lake, "hotspot", "event_date")
    out["records_in"] = records_in
    out["records_duplicate"] = records_in - df.count()
    return out


# ------------------------------------------------------------------ Open-Meteo ERA5

WEATHER_VARS = ("temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
                "precipitation", "boundary_layer_height")


def stg_weather(spark: SparkSession, lake: str) -> dict:
    raw = _read_raw_text(spark, f"{lake}/raw/source=openmeteo/dataset=era5_hourly", "*.json", "json",
                         multiLine="true")
    hourly_fields = set(raw.schema["hourly"].dataType.fieldNames())
    arrays = [F.col("hourly.time").alias("time")] + [
        (F.col(f"hourly.{v}") if v in hourly_fields else F.array().cast("array<double>")).alias(v)
        for v in WEATHER_VARS]
    fname = F.element_at(F.split("source_file", "/"), -1)
    df = (raw.select(F.regexp_extract(fname, r"(cell_-?\d+_-?\d+)", 1).alias("point_id"),
                     F.col("latitude").alias("grid_latitude"), F.col("longitude").alias("grid_longitude"),
                     F.arrays_zip(*arrays).alias("rows"), "source_file")
          .withColumn("r", F.explode("rows"))
          .select("point_id", "grid_latitude", "grid_longitude", "source_file",
                  F.to_timestamp("r.time").alias("event_ts"),   # Open-Meteo asked with timezone=UTC
                  *[F.col(f"r.{v}").cast("double").alias(v) for v in WEATHER_VARS])
          .withColumn("ingest_date", _ingest_date()))
    records_in = df.count()
    df = _latest_per_key(df, ["point_id", "event_ts"], [F.col("ingest_date").desc(), F.col("source_file").desc()])
    rule = _first_failed_rule(
        ("DQ-02", F.col("event_ts").isNull()),
        ("DQ-03", F.col("temperature_2m").isNull()),
    )
    df = df.withColumn("dq_rule", rule).withColumn("event_date", F.to_date("event_ts"))
    out = _write(df.filter("dq_rule IS NULL").drop("dq_rule"), df.filter("dq_rule IS NOT NULL"),
                 lake, "weather", "event_date")
    out["records_in"] = records_in
    out["records_duplicate"] = records_in - df.count()
    return out


# ------------------------------------------------------------------ GISTDA modeled PM2.5


def stg_gistda(spark: SparkSession, lake: str) -> dict:
    raw = _read_raw_text(spark, f"{lake}/raw/source=gistda/dataset=pm25_amphoe_hourly", "*.json", "json",
                         multiLine="true")
    fields = set(raw.schema["data"].dataType.elementType.fieldNames())

    def opt(name, typ="string"):
        return F.col(f"d.{name}").cast(typ) if name in fields else F.lit(None).cast(typ)

    fname = F.element_at(F.split("source_file", "/"), -1)
    df = (raw.filter(F.col("status") == 200)
          .select("source_file", F.explode("data").alias("d"))
          .select(
              F.when(fname == "province.json", "province").otherwise("amphoe").alias("admin_level"),
              F.coalesce(opt("ap_idn", "int"), opt("pv_idn", "int")).alias("admin_code"),
              F.when(fname == "province.json", F.lit(None).cast("int"))
               .otherwise(F.regexp_extract(fname, r"pv_idn=(\d+)", 1).cast("int")).alias("parent_code"),
              F.coalesce(opt("ap_tn"), opt("pv_tn")).alias("name_th"),
              F.coalesce(opt("ap_en"), opt("pv_en")).alias("name_en"),
              opt("pm25", "double").alias("pm25_modeled"),
              opt("pm25Avg24hr", "double").alias("pm25_avg_24h"),
              # GISTDA's "dt" says Z but is Thai local time (verified 2026-10-02): read clock time as ICT
              F.to_utc_timestamp(F.to_timestamp(F.substring(opt("dt"), 1, 19)), "Asia/Bangkok").alias("event_ts"),
              "source_file")
          .withColumn("ingest_date", _ingest_date()))
    records_in = df.count()
    df = _latest_per_key(df, ["admin_level", "admin_code", "event_ts"], [F.col("source_file").desc()])
    rule = _first_failed_rule(
        ("DQ-02", F.col("event_ts").isNull()),
        ("DQ-03", F.col("pm25_modeled").isNull()),
        ("DQ-06", F.col("pm25_modeled") < 0),
    )
    df = df.withColumn("dq_rule", rule).withColumn("event_date", F.to_date("event_ts"))
    out = _write(df.filter("dq_rule IS NULL").drop("dq_rule"), df.filter("dq_rule IS NOT NULL"),
                 lake, "gistda_pm25", "event_date")
    out["records_in"] = records_in
    out["records_duplicate"] = records_in - df.count()
    return out


JOBS = {"air_quality": stg_air_quality, "hotspot": stg_hotspot, "weather": stg_weather, "gistda": stg_gistda}

"""Curated jobs: staging -> analysis-ready Parquet (station-hour, station-day, hotspot exposure).

Each job rebuilds its dataset from staging (idempotent: same staging gives the same curated data).
Day boundaries use Asia/Bangkok ("local_date"); instants stay UTC everywhere else.

Rules applied here (design doc sections 12 and 17):
  * rolling 24 h PM2.5 mean needs >= MIN_HOURS_24H valid hours, otherwise it is NULL, never imputed;
  * a station-day PM2.5 mean needs >= MIN_HOURS_DAY valid hours, otherwise it is NULL;
  * hotspot exposure is a spatial join done as grid-bucket (1 deg cells) + exact haversine only on
    the candidates in neighbouring cells; tests compare it against brute force;
  * hotspot counts are 0 only inside the period FIRMS data actually covers (no false zeros).
"""

from __future__ import annotations

import math

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pm25.common.geo import EARTH_RADIUS_KM, THAILAND_BBOX

LOCAL_TZ = "Asia/Bangkok"
MIN_HOURS_24H = 18          # of the last 24 hourly slots
MIN_HOURS_DAY = 18          # of 24 hours in the local day
RADII_KM = (50, 100, 200)
POLLUTANT_COLS = {"PM2.5": "pm25", "PM10": "pm10", "O3": "o3", "NO2": "no2", "SO2": "so2", "CO": "co"}
WEATHER_COLS = ("temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m",
                "precipitation", "boundary_layer_height")


def _write(df: DataFrame, lake: str, dataset: str, partition: str) -> int:
    (df.repartition(partition).sortWithinPartitions(partition)
     .write.mode("overwrite").partitionBy(partition).parquet(f"{lake}/curated/{dataset}"))
    return df.sparkSession.read.parquet(f"{lake}/curated/{dataset}").count()


def _summary(records_in: int, records_out: int, **extra) -> dict:
    return {"records_in": records_in, "records_out": records_out, "records_quarantined": 0,
            "records_duplicate": 0, "quarantine_by_rule": {}, **extra}


def haversine_km_col(lat1, lon1, lat2, lon2):
    p1, p2 = F.radians(lat1), F.radians(lat2)
    a = (F.pow(F.sin((p2 - p1) / 2), 2)
         + F.cos(p1) * F.cos(p2) * F.pow(F.sin(F.radians(lon2 - lon1) / 2), 2))
    return F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))


# ------------------------------------------------------------------ station dimension (from staging)


def weather_point_id(lat, lon, cell_deg: float = 0.1):
    """ERA5 0.1-degree cell id; same rule as the downloader (backfill.sources.grid_points)."""
    def idx(c):
        return F.bround(c / cell_deg).cast("int").cast("string")
    return F.concat(F.lit("cell_"), idx(lat), F.lit("_"), idx(lon))


def air_quality_by_station(spark: SparkSession, lake: str) -> DataFrame:
    """Staging air quality with ONE station per physical site.

    OpenAQ can list the same Air4Thai site under several location ids (the provider re-registered it: same
    name apart from a numeric suffix, same coordinates, different sensors and periods). Counting each id as a
    station would inflate the station count and split one site's record across several ids. The smallest id of
    the group becomes the station; the id a row arrived with stays in source_location_id.
    """
    aq = spark.read.parquet(f"{lake}/staging/air_quality")
    group = Window.partitionBy("base_name", "lat_r", "lon_r")
    canon = (aq.groupBy("location_id").agg(F.max("location_name").alias("name"), F.max("latitude").alias("lat"),
                                           F.max("longitude").alias("lon"))
             .withColumn("base_name", F.regexp_replace("name", r"-\d+$", ""))
             .withColumn("lat_r", F.round("lat", 4)).withColumn("lon_r", F.round("lon", 4))
             .withColumn("canonical_location_id", F.min("location_id").over(group))
             .select("location_id", "canonical_location_id"))
    return (aq.join(F.broadcast(canon), "location_id")
              .withColumn("source_location_id", F.col("location_id"))
              .withColumn("location_id", F.col("canonical_location_id")).drop("canonical_location_id")
              .withColumn("station_id", F.concat(F.lit("openaq:"), F.col("location_id").cast("string"))))


def station_table(spark: SparkSession, lake: str) -> DataFrame:
    """One row per station: newest observed name and coordinates."""
    aq = air_quality_by_station(spark, lake)
    w = Window.partitionBy("station_id").orderBy(F.col("event_ts").desc())
    return (aq.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .select("station_id", "location_id", "location_name", "latitude", "longitude"))


# ------------------------------------------------------------------ station-hour


def cur_station_hour(spark: SparkSession, lake: str) -> dict:
    aq = air_quality_by_station(spark, lake)
    records_in = aq.count()
    hourly = aq.withColumn("hour_ts", F.date_trunc("hour", "event_ts")).groupBy("station_id", "hour_ts")
    df = hourly.agg(*[F.avg(F.when(F.col("pollutant") == name, F.col("value"))).alias(col)
                      for name, col in POLLUTANT_COLS.items()])
    df = df.filter(" OR ".join(f"{c} IS NOT NULL" for c in POLLUTANT_COLS.values()))

    # rolling 24 h PM2.5 (current hour + 23 before) on the instant axis, never across stations
    w = (Window.partitionBy("station_id").orderBy(F.col("hour_ts").cast("long")).rangeBetween(-23 * 3600, 0))
    df = (df.withColumn("pm25_24h_n", F.count("pm25").over(w))
            .withColumn("_avg24", F.avg("pm25").over(w))
            .withColumn("pm25_24h_mean", F.when(F.col("pm25_24h_n") >= MIN_HOURS_24H, F.col("_avg24")))
            .drop("_avg24"))

    # ERA5 grid cell of each station, derived from its coordinates
    pts = (station_table(spark, lake)
           .select("station_id", weather_point_id(F.col("latitude"), F.col("longitude")).alias("point_id")))
    wx = (spark.read.parquet(f"{lake}/staging/weather")
          .select("point_id", F.col("event_ts").alias("hour_ts"), *WEATHER_COLS))
    df = (df.join(pts, "station_id", "left").join(wx, ["point_id", "hour_ts"], "left"))
    df = df.withColumn("local_date", F.to_date(F.from_utc_timestamp("hour_ts", LOCAL_TZ)))
    df = df.withColumn("local_month", F.date_format("local_date", "yyyy-MM"))

    out = _write(df, lake, "station_hour", "local_month")
    keys = spark.read.parquet(f"{lake}/curated/station_hour").select("station_id", "hour_ts")
    dup = out - keys.distinct().count()
    return _summary(records_in, out, duplicate_keys=dup)


# ------------------------------------------------------------------ station-day


def cur_station_day(spark: SparkSession, lake: str) -> dict:
    sh = spark.read.parquet(f"{lake}/curated/station_hour")
    records_in = sh.count()
    aggs = [
        F.count("pm25").alias("pm25_hours"),
        F.avg("pm25").alias("_pm25_avg"),
        F.max("pm25").alias("pm25_max"),
        F.avg("pm10").alias("pm10_mean"),
        F.avg("temperature_2m").alias("temp_mean"),
        F.avg("relative_humidity_2m").alias("rh_mean"),
        F.avg("wind_speed_10m").alias("wind_speed_mean"),
        F.sum("precipitation").alias("precip_sum"),
        F.avg("boundary_layer_height").alias("blh_mean"),
        F.count("temperature_2m").alias("weather_hours"),
    ]
    df = (sh.groupBy("station_id", "local_date").agg(*aggs)
            .withColumn("pm25_mean", F.when(F.col("pm25_hours") >= MIN_HOURS_DAY, F.col("_pm25_avg")))
            .drop("_pm25_avg")
            .withColumn("local_month", F.date_format("local_date", "yyyy-MM")))
    df = df.filter(F.col("pm25_hours") > 0)       # a station-day is an analysis row only if PM2.5 was measured
    out = _write(df, lake, "station_day", "local_month")
    return _summary(records_in, out)


# ------------------------------------------------------------------ hotspot exposure (spatial join)


def neighbour_span(max_km: float, max_abs_lat: float = THAILAND_BBOX.north, cell_deg: float = 1.0) -> int:
    """How many 1-degree cells around a station can hold a point within max_km (worst case: highest latitude)."""
    km_per_deg_lon = 111.195 * math.cos(math.radians(max_abs_lat))
    return math.ceil(max_km / (km_per_deg_lon * cell_deg))


def hotspot_exposure(hotspots: DataFrame, stations: DataFrame, radii_km=RADII_KM) -> DataFrame:
    """Per station and local date: hotspot counts and FRP sums inside each radius.

    hotspots needs latitude, longitude, event_ts, frp, confidence; stations needs station_id, latitude, longitude.
    Returns rows only for (station, date) pairs with at least one hotspot within the largest radius.
    """
    span = neighbour_span(max(radii_km))
    st = stations.select("station_id", F.col("latitude").alias("s_lat"), F.col("longitude").alias("s_lon"))
    offsets = F.array(*[F.struct(F.lit(dy).alias("dy"), F.lit(dx).alias("dx"))
                        for dy in range(-span, span + 1) for dx in range(-span, span + 1)])
    st = (st.withColumn("o", F.explode(offsets))
            .withColumn("cell_lat", F.floor("s_lat").cast("int") + F.col("o.dy"))
            .withColumn("cell_lon", F.floor("s_lon").cast("int") + F.col("o.dx")).drop("o"))
    hs = (hotspots.select(
        F.col("latitude").alias("h_lat"), F.col("longitude").alias("h_lon"), "frp",
        (F.lower("confidence") == "h").alias("high_conf"),
        F.to_date(F.from_utc_timestamp("event_ts", LOCAL_TZ)).alias("local_date"))
        .withColumn("cell_lat", F.floor("h_lat").cast("int")).withColumn("cell_lon", F.floor("h_lon").cast("int")))
    cand = hs.join(F.broadcast(st), ["cell_lat", "cell_lon"])
    cand = cand.withColumn("dist_km", haversine_km_col(F.col("s_lat"), F.col("s_lon"), F.col("h_lat"), F.col("h_lon")))
    aggs = []
    for r in radii_km:
        inside = F.col("dist_km") <= r
        aggs += [F.count(F.when(inside, 1)).alias(f"hotspots_{r}km"),
                 F.coalesce(F.sum(F.when(inside, F.col("frp"))), F.lit(0.0)).alias(f"frp_sum_{r}km")]
    aggs.append(F.count(F.when((F.col("dist_km") <= 100) & F.col("high_conf"), 1)).alias("hotspots_high_conf_100km"))
    out = cand.groupBy("station_id", "local_date").agg(*aggs)
    return out.filter(F.col(f"hotspots_{max(radii_km)}km") > 0)


def cur_hotspot_station_day(spark: SparkSession, lake: str) -> dict:
    hs = spark.read.parquet(f"{lake}/staging/hotspot")
    records_in = hs.count()
    cover = hs.select(F.to_date(F.from_utc_timestamp("event_ts", LOCAL_TZ)).alias("d")).agg(
        F.min("d").alias("lo"), F.max("d").alias("hi")).first()
    days = (spark.read.parquet(f"{lake}/curated/station_day").select("station_id", "local_date")
            .filter(F.col("local_date").between(F.lit(cover["lo"]), F.lit(cover["hi"]))))   # FIRMS coverage only
    exposure = hotspot_exposure(hs, station_table(spark, lake))
    df = days.join(exposure, ["station_id", "local_date"], "left")
    metric_cols = [c for c in exposure.columns if c not in ("station_id", "local_date")]
    df = (df.fillna(0, subset=metric_cols)
            .withColumn("local_month", F.date_format("local_date", "yyyy-MM")))
    out = _write(df, lake, "hotspot_station_day", "local_month")
    return _summary(records_in, out, firms_from=str(cover["lo"]), firms_to=str(cover["hi"]))


JOBS = {"station_hour": cur_station_hour,
        "station_day": cur_station_day,
        "hotspot_station_day": cur_hotspot_station_day}


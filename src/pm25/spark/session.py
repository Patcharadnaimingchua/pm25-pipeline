"""Spark session for the pipeline: local mode inside the Airflow worker, reading/writing the lake.

Local mode keeps the 1-week scope realistic (no separate Spark cluster to operate) while the code is
the same DataFrame code that would run on a cluster — only `master` changes.
"""

from __future__ import annotations

import os

UTC_SETTINGS = {
    "spark.sql.session.timeZone": "UTC",                      # all event times are UTC in the lake
    "spark.sql.sources.partitionOverwriteMode": "dynamic",    # overwrite only the partitions we write
    "spark.sql.adaptive.enabled": "true",                     # AQE: coalesce shuffle partitions, split skew
    "spark.sql.shuffle.partitions": "16",                     # one machine, not the default 200
    # small-file problem: pack many ~0.3 KB files into one task instead of one task per file
    "spark.sql.files.openCostInBytes": str(64 * 1024),
    "spark.sql.files.maxPartitionBytes": str(64 * 1024 * 1024),
    "spark.sql.parquet.compression.codec": "zstd",
}


def lake_uri() -> str:
    """s3a://<bucket> in Docker; a local folder path works too (tests, fallback)."""
    return os.environ.get("LAKE_URI", f"s3a://{os.environ.get('LAKE_BUCKET', 'pm25-lake')}")


def get_spark(app: str, master: str | None = None, driver_memory: str = "2g"):
    from pyspark.sql import SparkSession

    builder = (SparkSession.builder.appName(app)
               .master(master or os.environ.get("SPARK_MASTER", "local[*]"))
               .config("spark.driver.memory", driver_memory)
               .config("spark.ui.showConsoleProgress", "false"))
    for k, v in UTC_SETTINGS.items():
        builder = builder.config(k, v)
    if lake_uri().startswith("s3a://"):
        endpoint = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
        builder = (builder
                   .config("spark.hadoop.fs.s3a.endpoint", endpoint)
                   .config("spark.hadoop.fs.s3a.path.style.access", "true")
                   .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
                   .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
                   .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
                   .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                   .config("spark.hadoop.fs.s3a.connection.maximum", "128")   # many tiny objects, read in parallel
                   .config("spark.hadoop.fs.s3a.threads.max", "64")
                   .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_DEFAULT_REGION", "us-east-1")))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

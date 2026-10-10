"""Thin S3 client for the lake (works with MinIO, SeaweedFS, AWS S3 — anything S3-compatible)."""

from __future__ import annotations

import logging

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pm25.settings import get_settings

log = logging.getLogger(__name__)


class ObjectAlreadyExists(RuntimeError):
    """Raised when code tries to overwrite an object under raw/ (raw is immutable)."""


def lake_client():
    s = get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint_url,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
    )


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def put_raw(client, bucket: str, key: str, data: bytes, content_type: str) -> None:
    """Write a raw object exactly once. Every run writes under its own run_id prefix,
    so a collision means a bug (or a re-used run id) — fail loudly instead of overwriting."""
    if not key.startswith("raw/"):
        raise ValueError(f"put_raw only writes under raw/, got {key!r}")
    if object_exists(client, bucket, key):
        raise ObjectAlreadyExists(key)
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    log.info("landed s3://%s/%s (%d bytes)", bucket, key, len(data))


def get_bytes(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def list_keys(client, bucket: str, prefix: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]

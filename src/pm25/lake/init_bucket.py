"""Create the lake bucket if missing (idempotent). Run by the `lake-init` compose service."""

from __future__ import annotations

import logging
import sys
import time

from botocore.exceptions import ClientError, EndpointConnectionError

from pm25.lake.s3 import lake_client
from pm25.settings import get_settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("lake-init")

ZONE_MARKERS = ("raw/", "staging/", "curated/", "quarantine/", "metadata/")


def main(retries: int = 30, wait_s: float = 2.0) -> int:
    bucket = get_settings().lake_bucket
    client = lake_client()
    for attempt in range(1, retries + 1):
        try:
            client.head_bucket(Bucket=bucket)
            log.info("bucket %s already exists", bucket)
            break
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchBucket", "NotFound"):
                client.create_bucket(Bucket=bucket)
                log.info("created bucket %s", bucket)
                break
            raise
        except EndpointConnectionError:
            log.info("waiting for object storage (%d/%d)...", attempt, retries)
            time.sleep(wait_s)
    else:
        log.error("object storage did not come up")
        return 1

    # Zero-byte markers make the zones visible in the console; harmless if they exist.
    for marker in ZONE_MARKERS:
        client.put_object(Bucket=bucket, Key=marker + ".keep", Body=b"")
    log.info("lake zones ready: %s", ", ".join(ZONE_MARKERS))
    return 0


if __name__ == "__main__":
    sys.exit(main())

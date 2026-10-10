# Runbook

## First start

1. `make init` → edit `.env` (passwords, keys, your e-mail in `HTTP_USER_AGENT`)
2. `make up` (first build pulls images and installs requirements: several minutes)
3. `make ps` → `airflow-init` and `lake-init` should show *Exited (0)*; everything else *running/healthy*
4. Airflow UI http://localhost:8080 (user/password from `.env`), MinIO console http://localhost:9001
5. In Airflow, **unpause `ingest_gistda_pm25_hourly`** and `ingest_gistda_pm25_province_24h_daily`
6. After the next :20, check MinIO → bucket `pm25-lake` → `raw/source=gistda/...` and
   `make dwh-psql` → `SELECT * FROM audit.pipeline_run ORDER BY started_at DESC LIMIT 5;`

## Keep the GISTDA DAG running

GISTDA serves only ~24 hours of history. Run the stack on a machine that stays on
(a desktop, a university VM). If it was off for more than 24 hours, record the gap:

```sql
INSERT INTO audit.data_gap (source_system, dataset, gap_start, gap_end, reason)
VALUES ('gistda', 'pm25_amphoe_hourly', '2026-10-10 00:00+00', '2026-10-11 06:00+00', 'host powered off');
```

## Object storage image

MinIO stopped publishing free community Docker images in October 2025. The compose file uses
Chainguard's free rebuilt image `cgr.dev/chainguard/minio:latest`. If that pull fails, replace the
`minio` service with SeaweedFS (S3-compatible, Apache-2.0) — only `S3_ENDPOINT_URL` and the
credentials change, no pipeline code changes:

```yaml
  minio:            # keep the service name so nothing else changes
    image: chrislusf/seaweedfs:latest
    command: "server -s3 -dir=/data"
    ports: ["8333:8333"]
    volumes: [minio-volume:/data]
```

Then set `S3_ENDPOINT_URL=http://minio:8333` in `.env`. Check the SeaweedFS documentation for how it
configures S3 credentials before relying on this; it was not tested in this scaffold.

## Least privilege (production note)

In development the pipeline uses the object-store root credentials. In production create a separate
user whose policy allows `PutObject`/`GetObject`/`ListBucket` on the bucket but **no `DeleteObject`
on `raw/*`**, and enable versioning/object lock on the raw zone.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `airflow-init` exits non-zero | `FERNET_KEY` / `AIRFLOW_JWT_SECRET` empty | `make init` again, then `make up` |
| Files owned by root on Linux | `AIRFLOW_UID` not set | `echo AIRFLOW_UID=$(id -u) >> .env` |
| `lake-init` keeps waiting | object storage did not start | `docker compose logs minio` |
| Task fails with `FatalHttpError 401/403` | wrong API key | fix `.env`, `docker compose up -d` (retries will not help) |
| `ObjectAlreadyExists` | a run id + try number was reused | should never happen; report it — raw is write-once by design |
| DWH tables missing | `sql/ddl` runs only on an empty volume | run the files manually with `make dwh-psql`, or `docker compose down -v` (deletes DWH data) |

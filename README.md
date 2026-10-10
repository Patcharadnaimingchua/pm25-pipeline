# PM2.5 Thailand Big Data Pipeline

Data platform สำหรับ ingest, store, process และ integrate ข้อมูล PM2.5 ของไทยร่วมกับ hotspot และ weather
Architecture: **Airflow → MinIO (data lake) → PySpark → PostgreSQL (star schema) → Power BI**
รายละเอียดการออกแบบทั้งหมดอยู่ใน design review (Claude Doc ของทีม)

## สถานะของ scaffold นี้ (Phase 0–2)

| ส่วน | สถานะ |
| --- | --- |
| Docker stack: Airflow 3.3.2 (LocalExecutor), MinIO, PostgreSQL DWH | พร้อม |
| Lake layout + raw แบบ write-once + manifest | พร้อม |
| `ingest_gistda_pm25_hourly` + `ingest_gistda_pm25_province_24h_daily` | พร้อม — **เปิดใช้ทันที** |
| `audit.pipeline_run`, `audit.dq_result`, `audit.data_gap` | พร้อม |
| สคริปต์ Gate G1 (นับ records จริง ≥ 1M) | พร้อม |
| Ingest DAG ของ OpenAQ / FIRMS / Open-Meteo | Week 2–3 (มี client + URL builder + tests แล้ว) |
| Spark jobs, DQ engine, star schema, Power BI | Week 4 เป็นต้นไป |

## เริ่มต้น (ทำตามลำดับ)

ต้องมี Docker Desktop (Windows ใช้ WSL2), RAM ว่างให้ Docker อย่างน้อย 8 GB และ Python 3.10+

```bash
# 1) secrets + โฟลเดอร์
make init                  # Windows ที่ไม่มี make: copy .env.example .env แล้ว python scripts/dev/fill_env_secrets.py .env
# แก้ .env: รหัสผ่าน, FIRMS_MAP_KEY, OPENAQ_API_KEY, อีเมลใน HTTP_USER_AGENT

# 2) เปิด stack
make up                    # = docker compose build airflow-init && docker compose up -d
make ps                    # airflow-init และ lake-init ต้อง Exited (0)

# 3) เปิด DAG ของ GISTDA ใน Airflow UI http://localhost:8080
#    ingest_gistda_pm25_hourly  และ  ingest_gistda_pm25_province_24h_daily

# 4) unit tests บนเครื่อง
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
make test
```

## Week 1: Gate G1 — นับ records จริง

Hard requirement: integrated workload ≥ **1,000,000 real records** ใน window 2022-01-01 → 2025-12-31
นับหลัง dedup ห้ามซ้ำ/ห้ามสังเคราะห์ ผลจะถูกเขียนต่อท้าย `docs/data_volume_verification.md` อัตโนมัติ

```bash
make week1-gistda              # นับจังหวัด/อำเภอจริงของ GISTDA
make week1-openaq-locations    # รายชื่อสถานี OpenAQ ในไทย -> seeds/openaq_th_locations.csv
make week1-openaq-download     # ดาวน์โหลด archive ของสถานี Air4Thai (S3, ไม่ต้องมี key)
make week1-openaq-count        # นับ row จริงด้วย DuckDB -> PASS/NOT YET
make week1-firms               # นับ hotspot VIIRS จริงใน bbox ไทย
```

## ดึงข้อมูลจำนวนมากอย่างมีจรรยาบรรณ (backfill)

ทุก request ผ่าน polite client ซึ่งคุม pacing, budget ต่ำกว่า limit ที่ประกาศ, robots.txt, `Retry-After`,
circuit breaker และบันทึกลง ledger — กติกาอยู่ใน `config/source_policies.yaml`, เหตุผลอยู่ใน `docs/data_ethics.md`

```bash
make backfill-openaq      # S3 archive ของสถานี Air4Thai (ไม่ดึงไฟล์ที่มีแล้ว)
make backfill-firms       # VIIRS SNPP + NOAA-20, ตามกติกา 5,000 transactions/10 นาที
make backfill-weather     # ERA5 ครั้งเดียวต่อ grid cell (ไม่ซ้ำต่อสถานี)
make backfill-push        # ส่งเข้า lake raw zone พร้อม manifest (ต้อง make up ก่อน)
make backfill-report      # peak ต่อนาที/ชั่วโมง/วัน ต่อ host เทียบ limit -> หลักฐานใน report
```

กด Ctrl+C ได้ทุกเมื่อ แล้วรันคำสั่งเดิมซ้ำเพื่อทำต่อจากจุดเดิม

## โครงสร้าง

```
dags/            Airflow DAGs (บางที่สุด: เรียกโค้ดใน src/)
src/pm25/        package ที่ Airflow, Spark และสคริปต์ใช้ร่วมกัน
  ingestion/     client ของแต่ละ source + polite.py (pacing, budgets, robots, ledger)
  backfill/      bulk download แบบ resumable + push เข้า lake + สรุป ledger
  lake/          path layout, manifest, S3 client, bucket init
  pipelines/     logic ของแต่ละ task (เทสต์ได้โดยไม่ต้องมี Airflow)
  audit/         audit.pipeline_run / dq_result
  common/        เวลา (UTC/ICT), geo (haversine, bbox, grid)
sql/ddl/         schema ของ DWH (รันอัตโนมัติครั้งแรกที่สร้าง volume)
config/          sources.yaml (ไม่มี secret)
scripts/week1/   Gate G1 verification
tests/           unit tests + fixtures จาก response จริง
docs/            runbook, data volume verification, data ethics
ATTRIBUTION.md   แหล่งข้อมูล, license, ข้อความให้เครดิต
```

## กติกาของทีม

- ห้าม commit `.env` หรือ key ใดๆ
- raw zone เขียนครั้งเดียว (`put_raw` จะ error ถ้าไฟล์มีอยู่แล้ว) ทุก retry เขียนใต้ prefix ของ try นั้น
- เวลาทั้งหมดใน lake เป็น UTC; แปลงเป็นเวลาไทยเฉพาะตอนสร้าง "วัน" และ dashboard
- DAG รายวันที่ต้อง backfill ได้ (OpenAQ, FIRMS, weather) ต้องใช้ `schedule=CronDataIntervalTimetable("0 19 * * *", timezone="UTC")`
  เพราะ cron string ปกติใน Airflow 3 ให้ `data_interval_start == data_interval_end` — `tests/dags` ตรวจข้อนี้ให้
- DAG integrity test: `PYTHONPATH=src python -m pytest tests/dags` (ต้องมี Airflow; ถ้าไม่มีจะ skip)
- ทุก PR ต้องผ่าน `make test` และมีคน review อย่างน้อย 1 คน
- ทุกการเพิ่ม component ใหม่ (เช่น Kafka, Trino) ต้องมี ADR ใน `docs/` ก่อน

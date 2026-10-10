.PHONY: help init build up down ps logs test lint dwh-psql week1-gistda week1-openaq-locations week1-openaq-download week1-openaq-count week1-firms backfill-openaq backfill-firms backfill-weather backfill-push backfill-report

PY ?= python3

help:
	@echo "init                    create .env with generated secrets + local folders"
	@echo "up / down / ps / logs   manage the Docker stack"
	@echo "test / lint             unit tests and linter (local venv)"
	@echo "dwh-psql                open psql on the warehouse"
	@echo "week1-*                 Gate G1 data-volume verification scripts (see README)"
	@echo "backfill-*              polite bulk download + push to lake + compliance report (docs/data_ethics.md)"

init:
	@test -f .env || cp .env.example .env
	@$(PY) scripts/dev/fill_env_secrets.py .env
	@mkdir -p logs airflow-config data
	@echo "Edit .env (passwords, FIRMS_MAP_KEY, OPENAQ_API_KEY, email in HTTP_USER_AGENT), then: make up"

build:
	docker compose build airflow-init

up: build
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080   MinIO console: http://localhost:9001   DWH: localhost:5433"

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src dags scripts tests

dwh-psql:
	docker compose exec postgres-dwh sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# ---------- Week 1: Gate G1 (run on the laptop, needs internet) ----------
week1-gistda:
	$(PY) scripts/week1/gistda_count_admin.py

week1-openaq-locations:
	$(PY) scripts/week1/openaq_th_locations.py

week1-openaq-download:
	$(PY) scripts/week1/openaq_download_window.py --start 2022-01-01 --end 2025-12-31 --provider Air4Thai

week1-openaq-count:
	$(PY) scripts/week1/count_openaq_rows.py --start 2022-01-01 --end 2025-12-31

week1-firms:
	$(PY) scripts/week1/firms_count.py --start 2022-01-01 --end 2025-12-31 --source VIIRS_SNPP_SP

# ---------- Polite bulk backfill (docs/data_ethics.md); resumable — re-run after any interruption ----------
WINDOW ?= --start 2022-01-01 --end 2025-12-31

backfill-openaq:
	$(PY) scripts/backfill/run.py openaq $(WINDOW) --provider Air4Thai

backfill-firms:
	$(PY) scripts/backfill/run.py firms $(WINDOW) --sources VIIRS_SNPP_SP VIIRS_NOAA20_SP

backfill-weather:
	$(PY) scripts/backfill/run.py weather $(WINDOW) --provider Air4Thai

backfill-push:
	$(PY) scripts/backfill/run.py push --source openaq
	$(PY) scripts/backfill/run.py push --source firms
	$(PY) scripts/backfill/run.py push --source openmeteo

backfill-report:
	$(PY) scripts/backfill/run.py report

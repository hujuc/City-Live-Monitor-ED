.PHONY: setup test lint typecheck kafka-up kafka-wait airflow-up airflow-wait serve-api stream-events stream-noise311 stream-aggregates silver-process-tlc silver-process-events silver-process-noise311 gold-build gold-build-star-schema report-data-profiling report-processing superset-up superset-bootstrap superset-export ingest-streaming silver-all pipeline reports docker-up docker-down docker-ps health clean clean-silver-gold demo-summary

# ── Setup ─────────────────────────────────────────────────────────────────────

setup:
	uv venv
	uv pip install -e ".[dev]"
	cp -n .env.example .env 2>/dev/null || true
	cp -n infra/airflow_simple_auth_passwords.json.example infra/airflow_simple_auth_passwords.json 2>/dev/null || true

# ── Quality ───────────────────────────────────────────────────────────────────

test:
	uv run pytest

lint:
	uv run ruff check src/ --fix
	uv run black src/

typecheck:
	uv run pyright src/

# ── API ───────────────────────────────────────────────────────────────────────

serve-api:
	uv run clm api --host 0.0.0.0 --port 8000

kafka-up:
	docker compose up -d kafka

kafka-wait: kafka-up
	@echo "Waiting for Kafka to be ready..."
	@for i in $$(seq 1 20); do \
		docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && exit 0; \
		sleep 2; \
	done; \
	echo "Kafka did not become ready in time"; \
	exit 1

airflow-up:
	docker compose up -d airflow-db airflow

airflow-wait: airflow-up
	@echo "Waiting for Airflow web UI to be ready..."
	@for i in $$(seq 1 30); do \
		code=$$(curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8080/ || true); \
		if [ "$$code" = "200" ] || [ "$$code" = "302" ]; then \
			echo "Airflow is ready at http://127.0.0.1:8080"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "Airflow did not become ready in time"; \
	docker compose logs --tail=80 airflow; \
	exit 1

stream-events: kafka-wait
	uv run clm stream-events --once

stream-noise311: kafka-wait
	uv run clm stream-noise311 --once

stream-aggregates: kafka-wait
	uv run clm stream-aggregates --once --max-messages 500

silver-process-tlc:
	uv run clm silver-process-tlc --taxi-type yellow

silver-process-events:
	uv run clm silver-process-events

silver-process-noise311:
	uv run clm silver-process-noise311

gold-build:
	uv run clm gold-build-all

gold-build-star-schema:
	uv run clm gold-build-star-schema

report-data-profiling:
	uv run clm report-data-profiling --sample-files 3

report-processing:
	uv run clm report-processing

backfill-bronze-meta:
	uv run clm backfill-bronze-meta

validate-bronze:
	uv run clm validate-bronze

superset-up:
	@PORT=$${SUPERSET_PORT:-8088}; \
	if ss -ltn "sport = :$$PORT" | tail -n +2 | grep -q .; then \
		echo "Port $$PORT is already in use. Falling back to port 8089."; \
		PORT=8089; \
	fi; \
	SUPERSET_PORT=$$PORT docker compose up -d superset-db superset; \
	echo "Superset running at http://localhost:$$PORT"

superset-bootstrap:
	@PORT=$$(docker compose port superset 8088 2>/dev/null | sed -n 's/.*://p' | head -n1); \
	if [ -z "$$PORT" ]; then PORT=$${SUPERSET_PORT:-8088}; fi; \
	echo "Bootstrapping Superset (host port: $$PORT)..."; \
	docker compose exec -e SUPERSET_PUBLIC_PORT=$$PORT superset bash /app/bootstrap.sh; \
	echo "Open Superset at http://localhost:$$PORT"

# Re-export dashboards after making UI changes, so they're committed to the repo
superset-export:
	docker compose exec superset superset export-dashboards -f /app/superset_home/assets.zip
	docker compose cp superset:/app/superset_home/assets.zip infra/superset/assets.zip

# ── Pipeline shortcuts ───────────────────────────────────────────────────────

# One-shot streaming ingest of both Socrata sources (requires Kafka)
ingest-streaming: stream-events stream-noise311

# Build all Silver partitions from Bronze (TLC + events + noise311)
silver-all: silver-process-tlc silver-process-events silver-process-noise311

# Full Bronze → Silver → Gold rebuild
pipeline: silver-all gold-build

# Regenerate markdown reports
reports: report-data-profiling report-processing

# ── Docker orchestration ────────────────────────────────────────────────────

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-ps:
	docker compose ps

# ── Health + summary ────────────────────────────────────────────────────────

health:
	@printf "API       "; curl -sf http://localhost:8000/health  >/dev/null && echo "OK" || echo "DOWN"
	@printf "Superset  "; curl -sf http://localhost:8088/health  >/dev/null && echo "OK" || echo "DOWN"
	@printf "Airflow   "; curl -sf http://localhost:8080/api/v2/version >/dev/null && echo "OK" || echo "DOWN"
	@printf "MinIO     "; curl -sf http://localhost:9000/minio/health/live >/dev/null && echo "OK" || echo "DOWN"

summary:
	@uv run clm summary

# ── Cleanup ─────────────────────────────────────────────────────────────────

clean:
	rm -rf htmlcov .coverage .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# CAUTION: wipes Silver + Gold. Bronze is preserved (slow to re-ingest).
clean-silver-gold:
	rm -rf data/silver/* data/gold/*
	@echo "Silver + Gold cleared. Run 'make pipeline' to rebuild."

.PHONY: setup test lint typecheck serve-api stream-events stream-noise311 process-tlc-batch silver-process-events silver-process-noise311 gold-build superset-up superset-bootstrap superset-export

# ── Setup ─────────────────────────────────────────────────────────────────────

setup:
	uv venv
	uv pip install -e ".[dev]"
	cp -n .env.example .env 2>/dev/null || true

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

stream-events:
	uv run clm stream-events --once

stream-noise311:
	uv run clm stream-noise311 --once

process-tlc-batch:
	uv run clm batch-process-tlc --taxi-type yellow

silver-process-events:
	uv run clm silver-process-events

silver-process-noise311:
	uv run clm silver-process-noise311

gold-build:
	uv run clm gold-build-all

superset-up:
	docker compose up -d superset-db superset

superset-bootstrap:
	docker compose exec superset bash /app/bootstrap.sh

# Re-export dashboards after making UI changes, so they're committed to the repo
superset-export:
	docker compose exec superset superset export-dashboards -f /app/superset_home/assets.zip
	docker compose cp superset:/app/superset_home/assets.zip infra/superset/assets.zip

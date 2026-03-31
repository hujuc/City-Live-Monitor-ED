.PHONY: setup test lint typecheck serve-api stream-events stream-noise311

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

#!/usr/bin/env bash
# Bootstrap Superset: DB migrations, admin user, DuckDB views, and registration
# in the metadata DB. Safe to re-run — all operations are idempotent.
#
# Invoked once after `docker compose up superset` has completed its first boot.
set -euo pipefail

ADMIN_USER=${SUPERSET_ADMIN_USER:-admin}
ADMIN_PASSWORD=${SUPERSET_ADMIN_PASSWORD:-admin}
ADMIN_EMAIL=${SUPERSET_ADMIN_EMAIL:-admin@example.com}
PUBLIC_PORT=${SUPERSET_PUBLIC_PORT:-8088}
DUCKDB_FILE=/app/superset_home/gold.duckdb
DB_NAME="gold_duckdb"

echo "[bootstrap] Running Superset DB migrations…"
superset db upgrade

echo "[bootstrap] Creating admin user (if missing)…"
superset fab create-admin \
    --username "$ADMIN_USER" \
    --firstname Admin \
    --lastname User \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD" || true

echo "[bootstrap] Initializing Superset roles/permissions…"
superset init

echo "[bootstrap] Creating DuckDB views over Gold Parquet files…"
/app/.venv/bin/python - <<PY
import duckdb
con = duckdb.connect("$DUCKDB_FILE")
with open("/app/init_gold_views.sql") as f:
    con.execute(f.read())
print("Views:", [r[0] for r in con.execute("SHOW TABLES").fetchall()])
con.close()
PY

if [ -f /app/assets.zip ]; then
    echo "[bootstrap] Importing dashboards/charts/datasets from assets.zip…"
    superset import-dashboards -p /app/assets.zip -u "$ADMIN_USER" || \
        echo "[bootstrap] (import may have skipped existing assets — this is fine)"
fi

echo "[bootstrap] Done."
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  Login:    http://localhost:${PUBLIC_PORT}"
echo "  User:     $ADMIN_USER"
echo "  Password: $ADMIN_PASSWORD"
echo "═══════════════════════════════════════════════════════════════════"

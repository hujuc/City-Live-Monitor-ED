FROM apache/superset:6.0.0

USER root

# DuckDB engine allows Superset to read our Parquet Gold tables directly.
# Superset 6 uses `uv` + a pip-less venv at /app/.venv/, so we install via uv.
RUN uv pip install --python /app/.venv/bin/python3 --no-cache \
    duckdb==1.1.3 \
    duckdb-engine==0.14.0

USER superset

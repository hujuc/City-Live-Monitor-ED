FROM python:3.12-slim

# Copy uv binary from the official image — avoids pip install issues in slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
# Install only third-party dependencies — skip building the local package,
# since PYTHONPATH=/app/src makes the source importable directly.
RUN UV_SYSTEM_PYTHON=1 uv sync --no-dev --no-install-project

COPY config/ config/
COPY src/backend/ src/backend/

ENV PYTHONPATH=/app/src
ENV PATH="/app/.venv/bin:$PATH"

CMD ["uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install uv --no-cache-dir && uv sync --no-dev

COPY config/ config/
COPY src/backend/ src/backend/

ENV PYTHONPATH=/app/src

CMD ["uv", "run", "uvicorn", "backend.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

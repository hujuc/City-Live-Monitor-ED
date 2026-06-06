"""Configuration module available from installed package path."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration from environment variables."""

    # Project paths (this file lives in src/config/settings.py)
    PROJECT_ROOT = Path(__file__).parents[2]
    DATA_DIR = PROJECT_ROOT / "data"
    DOCS_DIR = PROJECT_ROOT / "docs"
    REPORTS_DIR = PROJECT_ROOT / "docs" / "reports"
    BRONZE_DIR = DATA_DIR / "bronze"
    SILVER_DIR = DATA_DIR / "silver"
    GOLD_DIR = DATA_DIR / "gold"
    CHECKPOINTS_DIR = DATA_DIR / "checkpoints"
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_EVENTS_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "events.raw")
    KAFKA_NOISE311_TOPIC = os.getenv("KAFKA_NOISE311_TOPIC", "noise311.raw")
    KAFKA_EVENTS_DLQ_TOPIC = os.getenv("KAFKA_EVENTS_DLQ_TOPIC", "events.dlq")
    KAFKA_NOISE311_DLQ_TOPIC = os.getenv("KAFKA_NOISE311_DLQ_TOPIC", "noise311.dlq")

    # Streaming sources
    SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")
    EVENTS_API_URL = os.getenv(
        "EVENTS_API_URL", "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"
    )
    EVENTS_HISTORICAL_API_URL = os.getenv(
        "EVENTS_HISTORICAL_API_URL",
        "https://data.cityofnewyork.us/resource/bkfu-528j.json",
    )
    EVENTS_BACKFILL_START = os.getenv("EVENTS_BACKFILL_START", "2020-01-01T00:00:00")
    NOISE311_API_URL = os.getenv(
        "NOISE311_API_URL", "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    )
    NOISE311_BACKFILL_START = os.getenv("NOISE311_BACKFILL_START", "2024-06-01T00:00:00")
    EVENTS_POLL_SECONDS = int(os.getenv("EVENTS_POLL_SECONDS", "1800"))
    NOISE311_POLL_SECONDS = int(os.getenv("NOISE311_POLL_SECONDS", "30"))
    EVENTS_API_LIMIT = int(os.getenv("EVENTS_API_LIMIT", "2000"))
    NOISE311_API_LIMIT = int(os.getenv("NOISE311_API_LIMIT", "2000"))
    STREAM_REQUEST_TIMEOUT_SECONDS = int(os.getenv("STREAM_REQUEST_TIMEOUT_SECONDS", "30"))
    STREAM_CIRCUIT_BREAKER_FAILURES = int(os.getenv("STREAM_CIRCUIT_BREAKER_FAILURES", "5"))
    STREAM_CIRCUIT_BREAKER_COOLDOWN_SECONDS = int(
        os.getenv("STREAM_CIRCUIT_BREAKER_COOLDOWN_SECONDS", "120")
    )
    EVENTS_CHECKPOINT_FILE = Path(
        os.getenv("EVENTS_CHECKPOINT_FILE", str(CHECKPOINTS_DIR / "events_start_date_time.txt"))
    )
    NOISE311_CHECKPOINT_FILE = Path(
        os.getenv("NOISE311_CHECKPOINT_FILE", str(CHECKPOINTS_DIR / "noise311_created_date.txt"))
    )
    STREAM_AGGREGATOR_GROUP_ID = os.getenv("STREAM_AGGREGATOR_GROUP_ID", "stream-aggregator-v1")
    STREAM_AGGREGATOR_AUTO_OFFSET_RESET = os.getenv("STREAM_AGGREGATOR_AUTO_OFFSET_RESET", "latest")
    STREAM_AGGREGATOR_POLL_TIMEOUT_MS = int(os.getenv("STREAM_AGGREGATOR_POLL_TIMEOUT_MS", "1000"))
    STREAM_AGGREGATOR_MAX_EMPTY_POLLS = int(os.getenv("STREAM_AGGREGATOR_MAX_EMPTY_POLLS", "10"))
    STREAM_AGGREGATOR_WINDOW_MINUTES = int(os.getenv("STREAM_AGGREGATOR_WINDOW_MINUTES", "5"))
    STREAM_AGGREGATOR_TABLE_NAME = os.getenv("STREAM_AGGREGATOR_TABLE_NAME", "stream_metrics_5m")
    STREAM_AGGREGATOR_DAEMON_SLEEP_SECONDS = int(
        os.getenv("STREAM_AGGREGATOR_DAEMON_SLEEP_SECONDS", "5")
    )

    # MinIO (S3-compatible lakehouse)
    # Set MINIO_ENABLED=true only when running with Docker Compose (MinIO service up).
    # Defaults to false so local/CI runs never attempt a network connection.
    MINIO_ENABLED = os.getenv("MINIO_ENABLED", "false").lower() == "true"
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"
    MINIO_GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "gold-layer")
    MINIO_BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "bronze-layer")

    # API server
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", str(LOGS_DIR / "pipeline.log"))

    @classmethod
    def ensure_directories(cls) -> None:
        """Create necessary directories if they don't exist."""
        cls.BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        cls.SILVER_DIR.mkdir(parents=True, exist_ok=True)
        cls.GOLD_DIR.mkdir(parents=True, exist_ok=True)
        cls.CHECKPOINTS_DIR.mkdir(parents=True, exist_ok=True)
        cls.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        cls.DOCS_DIR.mkdir(parents=True, exist_ok=True)
        cls.REPORTS_DIR.mkdir(parents=True, exist_ok=True)


Config.ensure_directories()

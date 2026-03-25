"""
Configuration Module

Handles environment variables and application configuration.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Application configuration from environment variables."""

    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    BRONZE_DIR = DATA_DIR / "bronze"
    SILVER_DIR = DATA_DIR / "silver"
    GOLD_DIR = DATA_DIR / "gold"
    LOGS_DIR = PROJECT_ROOT / "logs"

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

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
        cls.LOGS_DIR.mkdir(parents=True, exist_ok=True)


Config.ensure_directories()

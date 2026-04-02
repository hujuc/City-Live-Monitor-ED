"""Kafka helpers for JSON event publishing."""

import json
from typing import Any, Protocol


class KafkaProducerLike(Protocol):
    """Protocol for producers used by pollers (real or test doubles)."""

    def send(self, topic: str, key: str, payload: dict[str, Any]) -> None: ...

    def close(self) -> None: ...


class JsonKafkaProducer:
    """Small JSON Kafka producer wrapper with stable key handling."""

    def __init__(self, bootstrap_servers: str) -> None:
        try:
            from kafka import KafkaProducer
            from kafka.errors import NoBrokersAvailable
        except ImportError as exc:
            raise RuntimeError(
                "kafka-python is required for streaming ingestion. "
                "Install dependencies with: uv pip install -e '.[dev]'"
            ) from exc

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
                    "utf-8"
                ),
                key_serializer=lambda key: key.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=50,
            )
        except NoBrokersAvailable as exc:
            raise RuntimeError(
                "Kafka broker is unavailable at "
                f"'{bootstrap_servers}'. Start Kafka first (e.g. 'docker compose up -d kafka')."
            ) from exc

    def send(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        """Publish one JSON payload and block until broker acknowledges it."""
        future = self._producer.send(topic=topic, key=key, value=payload)
        future.get(timeout=30)

    def close(self) -> None:
        """Flush and close producer connection."""
        self._producer.flush(timeout=10)
        self._producer.close()

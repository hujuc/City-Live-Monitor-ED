"""Incremental ingestion for NYC 311 Noise complaints (Socrata erm2-nwe9)."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger
from requests import RequestException

from backend.core.ingester import BaseIngester
from backend.ingestion.streaming.common import (
    CircuitBreaker,
    fetch_socrata_rows_paginated,
    make_event_id,
    parse_iso8601_or_now,
    write_ndjson_snapshot,
)
from backend.ingestion.streaming.kafka_client import JsonKafkaProducer, KafkaProducerLike
from config.settings import Config


class Noise311Poller(BaseIngester):
    """Polls 311 complaints incrementally and publishes noise events to Kafka."""

    def __init__(self, producer: KafkaProducerLike | None = None) -> None:
        self._producer = producer or JsonKafkaProducer(Config.KAFKA_BOOTSTRAP_SERVERS)
        self._breaker = CircuitBreaker(
            max_failures=Config.STREAM_CIRCUIT_BREAKER_FAILURES,
            cooldown_seconds=Config.STREAM_CIRCUIT_BREAKER_COOLDOWN_SECONDS,
        )
        self._checkpoint_file = Config.NOISE311_CHECKPOINT_FILE

    @property
    def source_name(self) -> str:
        return "nyc_311_noise"

    def ingest(self, destination: Path | None = None) -> int:
        if not self._breaker.before_call():
            logger.warning("[noise311_poller] Circuit open; skipping this cycle")
            return 0

        checkpoint = self._read_checkpoint()
        try:
            rows, max_created_date = fetch_socrata_rows_paginated(
                endpoint=Config.NOISE311_API_URL,
                timestamp_field="created_date",
                static_where="complaint_type like 'Noise%'",
                app_token=Config.SOCRATA_APP_TOKEN,
                timeout_seconds=Config.STREAM_REQUEST_TIMEOUT_SECONDS,
                limit=Config.NOISE311_API_LIMIT,
                cursor=checkpoint or Config.NOISE311_BACKFILL_START,
            )
            self._breaker.on_success()
        except (RequestException, ValueError) as exc:
            self._breaker.on_failure()
            logger.error("[noise311_poller] Poll failed: {}", exc)
            return 0

        snapshot_root = destination or Config.BRONZE_DIR
        snapshot_path = write_ndjson_snapshot(snapshot_root, self.source_name, rows)
        logger.info("[noise311_poller] Wrote Bronze snapshot: {}", snapshot_path)

        accepted = 0
        publish_errors = 0

        for row in rows:
            envelope = self._normalize(row)
            if envelope is None:
                self._send_dlq(reason="malformed_event", raw=row)
                continue

            try:
                self._producer.send(
                    topic=Config.KAFKA_NOISE311_TOPIC,
                    key=envelope["event_id"],
                    payload=envelope,
                )
                accepted += 1
            except Exception as exc:
                logger.error("[noise311_poller] Kafka publish failed: {}", exc)
                self._send_dlq(reason="kafka_publish_failed", raw=row)
                publish_errors += 1
                continue

        if publish_errors == 0 and max_created_date:
            self._write_checkpoint(max_created_date)
        elif publish_errors > 0:
            logger.warning(
                "[noise311_poller] {} publish error(s); checkpoint not advanced",
                publish_errors,
            )

        logger.info(
            "[noise311_poller] Published {} event(s) to {}", accepted, Config.KAFKA_NOISE311_TOPIC
        )
        return accepted

    def close(self) -> None:
        self._producer.close()

    def _normalize(self, row: dict[str, Any]) -> dict[str, Any] | None:
        created_date = row.get("created_date")
        if not created_date:
            return None

        try:
            event_id = make_event_id(
                source=self.source_name,
                row=row,
                preferred_fields=[
                    "unique_key",
                    "created_date",
                    "incident_zip",
                    "complaint_type",
                    "descriptor",
                ],
            )
            timestamp = parse_iso8601_or_now(created_date)
            return {
                "event_id": event_id,
                "source": self.source_name,
                "timestamp": timestamp.isoformat(),
                "received_at": datetime.now(tz=UTC).isoformat(),
                "payload": row,
            }
        except Exception as exc:
            logger.warning("[noise311_poller] Malformed event: {}", exc)
            return None

    def _read_checkpoint(self) -> str | None:
        if not self._checkpoint_file.exists():
            return None
        raw = self._checkpoint_file.read_text(encoding="utf-8").strip()
        return raw or None

    def _write_checkpoint(self, created_date_iso: str) -> None:
        self._checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
        self._checkpoint_file.write_text(created_date_iso, encoding="utf-8")

    def _send_dlq(self, reason: str, raw: dict[str, Any]) -> None:
        payload = {
            "reason": reason,
            "source": self.source_name,
            "received_at": datetime.now(tz=UTC).isoformat(),
            "payload": raw,
        }
        try:
            key = make_event_id(self.source_name, raw, ["unique_key", "created_date"])
            self._producer.send(Config.KAFKA_NOISE311_DLQ_TOPIC, key=key, payload=payload)
        except Exception as exc:
            logger.error("[noise311_poller] DLQ publish failed: {}", exc)

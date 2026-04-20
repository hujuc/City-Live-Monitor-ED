"""Kafka stream aggregation runner.

Consumes raw streaming events from Kafka topics, applies EventProcessor rules,
and persists 5-minute rolling metrics into a Gold table.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Callable

import pandas as pd
from loguru import logger

from backend.processing.streaming.event_processor import EventProcessor
from backend.storage.gold import GoldStorage
from config.settings import Config

ConsumerFactory = Callable[[], Any]


class KafkaAggregateRunner:
    """Consume events from Kafka and persist windowed streaming metrics."""

    def __init__(
        self,
        processor: EventProcessor | None = None,
        gold_storage: GoldStorage | None = None,
        consumer_factory: ConsumerFactory | None = None,
    ) -> None:
        self._processor = processor or EventProcessor(
            window_size=timedelta(minutes=Config.STREAM_AGGREGATOR_WINDOW_MINUTES)
        )
        self._gold = gold_storage or GoldStorage()
        self._consumer_factory = consumer_factory

    def run_once(self, max_messages: int = 500) -> dict[str, int]:
        """Poll Kafka in bounded mode, aggregate accepted events, and persist metrics."""
        consumer = self._build_consumer()
        seen = 0
        empty_polls = 0

        try:
            while seen < max_messages and empty_polls < Config.STREAM_AGGREGATOR_MAX_EMPTY_POLLS:
                records = consumer.poll(
                    timeout_ms=Config.STREAM_AGGREGATOR_POLL_TIMEOUT_MS,
                    max_records=min(100, max_messages - seen),
                )
                if not records:
                    empty_polls += 1
                    continue

                empty_polls = 0
                for partition_records in records.values():
                    for message in partition_records:
                        payload = self._decode_message_value(message.value)
                        self._processor.process_event(payload)
                        seen += 1
                        if seen >= max_messages:
                            break
                    if seen >= max_messages:
                        break
        finally:
            consumer.close()

        rows_written = self._persist_metrics()
        logger.info(
            "[kafka_aggregate_runner] seen={} rows_written={} max_messages={}",
            seen,
            rows_written,
            max_messages,
        )
        return {"seen": seen, "rows_written": rows_written}

    def _build_consumer(self) -> Any:
        if self._consumer_factory is not None:
            return self._consumer_factory()

        try:
            from kafka import KafkaConsumer
            from kafka.errors import NoBrokersAvailable
        except ImportError as exc:
            raise RuntimeError(
                "kafka-python is required for stream aggregation. "
                "Install dependencies with: uv pip install -e '.[dev]'"
            ) from exc

        try:
            return KafkaConsumer(
                Config.KAFKA_EVENTS_TOPIC,
                Config.KAFKA_NOISE311_TOPIC,
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=Config.STREAM_AGGREGATOR_GROUP_ID,
                enable_auto_commit=True,
                auto_offset_reset=Config.STREAM_AGGREGATOR_AUTO_OFFSET_RESET,
                value_deserializer=lambda value: self._decode_message_value(value),
            )
        except NoBrokersAvailable as exc:
            raise RuntimeError(
                "Kafka broker is unavailable at "
                f"'{Config.KAFKA_BOOTSTRAP_SERVERS}'. Start Kafka first "
                "(e.g. 'docker compose up -d kafka')."
            ) from exc

    def _persist_metrics(self) -> int:
        metrics_df = self._processor.metrics_dataframe()
        if metrics_df.empty:
            self._processor.reset_metrics()
            return 0

        table = Config.STREAM_AGGREGATOR_TABLE_NAME
        merged = metrics_df
        if self._gold.exists(table):
            existing = self._gold.read(table)
            merged = pd.concat([existing, metrics_df], ignore_index=True)

        key_cols = ["window_start_utc", "window_end_utc", "source", "subject"]
        aggregated = (
            merged.groupby(key_cols, as_index=False, dropna=False)
            .agg(
                event_count=("event_count", "sum"),
                late_event_count=("late_event_count", "sum"),
                duplicate_event_count=("duplicate_event_count", "sum"),
                out_of_order_event_count=("out_of_order_event_count", "sum"),
            )
            .sort_values(["window_start_utc", "source", "subject"])
            .reset_index(drop=True)
        )
        aggregated["late_event_ratio"] = (
            aggregated["late_event_count"] / aggregated["event_count"].replace(0, pd.NA)
        ).fillna(0.0)

        self._gold.write(aggregated, table)
        self._processor.reset_metrics()
        return len(aggregated)

    def _decode_message_value(self, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, (bytes, bytearray)):
            return json.loads(value.decode("utf-8"))
        if isinstance(value, str):
            return json.loads(value)
        raise ValueError(f"Unsupported Kafka message payload type: {type(value)}")

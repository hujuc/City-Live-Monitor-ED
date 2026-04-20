"""Tests for src/backend/orchestration/kafka_aggregate_runner.py"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from backend.processing.streaming import EventProcessor
from backend.processing.streaming.kafka_aggregate_runner import KafkaAggregateRunner
from backend.storage.gold import GoldStorage


@dataclass
class _FakeConsumer:
    polls: list[dict[str, list[SimpleNamespace]]]

    def __post_init__(self) -> None:
        self._index = 0

    def poll(self, timeout_ms: int, max_records: int) -> dict[str, list[SimpleNamespace]]:
        if self._index >= len(self.polls):
            return {}
        data = self.polls[self._index]
        self._index += 1
        return data

    def close(self) -> None:
        return


def _make_payload(event_id: str, source: str, offset_seconds: int = 0) -> dict:
    ts = datetime.now(tz=UTC) - timedelta(seconds=offset_seconds)
    return {
        "event_id": event_id,
        "source": source,
        "timestamp": ts.isoformat(),
        "payload": {"complaint_type": "Noise - Residential"},
    }


def _make_runner(tmp_path: Path, polls: list[dict[str, list[SimpleNamespace]]]) -> KafkaAggregateRunner:
    storage = GoldStorage(root=tmp_path / "gold")
    processor = EventProcessor()
    consumer = _FakeConsumer(polls=polls)
    return KafkaAggregateRunner(
        processor=processor,
        gold_storage=storage,
        consumer_factory=lambda: consumer,
    )


def test_run_once_processes_messages_and_persists(tmp_path: Path) -> None:
    polls = [
        {
            "topic-partition": [
                SimpleNamespace(value=_make_payload("evt-1", "nyc_311_noise")),
                SimpleNamespace(value=_make_payload("evt-2", "nyc_permitted_events")),
            ]
        },
        {},
    ]
    runner = _make_runner(tmp_path, polls)

    stats = runner.run_once(max_messages=50)

    assert stats["seen"] == 2
    assert stats["rows_written"] >= 1


def test_run_once_counts_duplicate_events(tmp_path: Path) -> None:
    payload = _make_payload("evt-dup", "nyc_311_noise")
    polls = [{"topic-partition": [SimpleNamespace(value=payload), SimpleNamespace(value=payload)]}, {}]
    runner = _make_runner(tmp_path, polls)

    stats = runner.run_once(max_messages=10)

    assert stats["seen"] == 2
    metrics = GoldStorage(root=tmp_path / "gold").read("stream_metrics_5m")
    assert int(metrics["duplicate_event_count"].sum()) == 1


def test_run_once_does_not_exit_on_first_empty_poll(tmp_path: Path, monkeypatch) -> None:
    # Lower empty poll threshold for a quick deterministic test.
    monkeypatch.setattr("config.settings.Config.STREAM_AGGREGATOR_MAX_EMPTY_POLLS", 2)
    polls = [{}, {}, {}]
    runner = _make_runner(tmp_path, polls)

    stats = runner.run_once(max_messages=10)

    assert stats["seen"] == 0
    assert stats["rows_written"] == 0

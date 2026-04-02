"""Tests for streaming pagination and checkpoint safety."""

from pathlib import Path

from backend.ingestion.streaming import common
from backend.ingestion.streaming.noise311_poller import Noise311Poller
from backend.ingestion.streaming.permitted_events_poller import PermittedEventsPoller


class _DummyProducer:
    def __init__(self, fail_on_send: int | None = None) -> None:
        self.fail_on_send = fail_on_send
        self.calls = 0

    def send(self, topic: str, key: str, payload: dict) -> None:
        self.calls += 1
        if self.fail_on_send is not None and self.calls == self.fail_on_send:
            raise RuntimeError("simulated kafka failure")

    def close(self) -> None:
        return None


def test_fetch_socrata_rows_paginated_multiple_pages(monkeypatch) -> None:
    calls: list[dict] = []
    pages = [
        [{"created_date": "2026-01-01T00:00:01"}, {"created_date": "2026-01-01T00:00:02"}],
        [{"created_date": "2026-01-01T00:00:03"}, {"created_date": "2026-01-01T00:00:04"}],
        [{"created_date": "2026-01-01T00:00:05"}],
    ]

    def _fake_fetch(*, endpoint, params, app_token, timeout_seconds):
        calls.append(params)
        return pages[len(calls) - 1]

    monkeypatch.setattr(common, "fetch_socrata_rows", _fake_fetch)

    rows, max_ts = common.fetch_socrata_rows_paginated(
        endpoint="http://example",
        timestamp_field="created_date",
        static_where="complaint_type like 'Noise%'",
        app_token=None,
        timeout_seconds=30,
        limit=2,
        cursor="2026-01-01T00:00:00",
        first_page_inclusive=False,
    )

    assert len(rows) == 5
    assert max_ts == "2026-01-01T00:00:05"
    assert (
        calls[0]["$where"]
        == "complaint_type like 'Noise%' AND created_date > '2026-01-01T00:00:00'"
    )
    assert (
        calls[1]["$where"]
        == "complaint_type like 'Noise%' AND created_date > '2026-01-01T00:00:02'"
    )
    assert (
        calls[2]["$where"]
        == "complaint_type like 'Noise%' AND created_date > '2026-01-01T00:00:04'"
    )


def test_fetch_socrata_rows_paginated_first_page_inclusive(monkeypatch) -> None:
    calls: list[dict] = []

    def _fake_fetch(*, endpoint, params, app_token, timeout_seconds):
        calls.append(params)
        return [{"start_date_time": "2026-01-01T00:00:00"}]

    monkeypatch.setattr(common, "fetch_socrata_rows", _fake_fetch)

    rows, max_ts = common.fetch_socrata_rows_paginated(
        endpoint="http://example",
        timestamp_field="start_date_time",
        static_where="",
        app_token=None,
        timeout_seconds=30,
        limit=2000,
        cursor="2026-01-01T00:00:00",
        first_page_inclusive=True,
    )

    assert len(rows) == 1
    assert max_ts == "2026-01-01T00:00:00"
    assert calls[0]["$where"] == "start_date_time >= '2026-01-01T00:00:00'"


def test_events_checkpoint_not_advanced_on_publish_error(monkeypatch, tmp_path: Path) -> None:
    poller = PermittedEventsPoller(producer=_DummyProducer(fail_on_send=2))
    poller._checkpoint_file = tmp_path / "events_checkpoint.txt"
    poller._checkpoint_file.write_text("2026-01-01T00:00:00", encoding="utf-8")

    rows = [
        {"event_name": "A", "event_location": "X", "start_date_time": "2026-01-01T00:00:01"},
        {"event_name": "B", "event_location": "X", "start_date_time": "2026-01-01T00:00:02"},
    ]

    monkeypatch.setattr(
        "backend.ingestion.streaming.permitted_events_poller.fetch_socrata_rows_paginated",
        lambda **_: (rows, "2026-01-01T00:00:02"),
    )
    monkeypatch.setattr(
        "backend.ingestion.streaming.permitted_events_poller.write_ndjson_snapshot",
        lambda root, source, payload: tmp_path / "events.ndjson",
    )

    accepted = poller.ingest(destination=tmp_path)
    checkpoint = poller._checkpoint_file.read_text(encoding="utf-8").strip()

    assert accepted == 1
    assert checkpoint == "2026-01-01T00:00:00"


def test_noise_checkpoint_advanced_on_success(monkeypatch, tmp_path: Path) -> None:
    poller = Noise311Poller(producer=_DummyProducer())
    poller._checkpoint_file = tmp_path / "noise_checkpoint.txt"
    poller._checkpoint_file.write_text("2026-01-01T00:00:00", encoding="utf-8")

    rows = [
        {"unique_key": "1", "created_date": "2026-01-01T00:00:01", "complaint_type": "Noise"},
        {"unique_key": "2", "created_date": "2026-01-01T00:00:02", "complaint_type": "Noise"},
    ]

    monkeypatch.setattr(
        "backend.ingestion.streaming.noise311_poller.fetch_socrata_rows_paginated",
        lambda **_: (rows, "2026-01-01T00:00:02"),
    )
    monkeypatch.setattr(
        "backend.ingestion.streaming.noise311_poller.write_ndjson_snapshot",
        lambda root, source, payload: tmp_path / "noise.ndjson",
    )

    accepted = poller.ingest(destination=tmp_path)
    checkpoint = poller._checkpoint_file.read_text(encoding="utf-8").strip()

    assert accepted == 2
    assert checkpoint == "2026-01-01T00:00:02"


def test_noise_checkpoint_with_timezone_is_normalized_on_read(tmp_path: Path) -> None:
    poller = Noise311Poller(producer=_DummyProducer())
    poller._checkpoint_file = tmp_path / "noise_checkpoint.txt"
    poller._checkpoint_file.write_text("2020-01-02T14:22:38+00:00", encoding="utf-8")

    checkpoint = poller._read_checkpoint()

    assert checkpoint == "2020-01-02T14:22:38"

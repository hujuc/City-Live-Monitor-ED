"""Tests for src/backend/processing/streaming/event_processor.py"""

from datetime import datetime, timedelta, timezone
import pytest
from backend.processing.streaming.event_processor import EventProcessor, EventEnvelope


def _make_raw(
    event_id: str = "evt-1",
    source: str = "311",
    offset_seconds: int = 0,
) -> dict:
    ts = datetime.now(tz=timezone.utc) - timedelta(seconds=offset_seconds)
    return {
        "event_id": event_id,
        "source": source,
        "timestamp": ts.isoformat(),
        "payload": {"complaint_type": "Noise - Residential"},
    }


class TestEventProcessorAccept:
    def test_valid_event_returns_envelope(self) -> None:
        proc = EventProcessor()
        result = proc.process_event(_make_raw())
        assert isinstance(result, EventEnvelope)
        assert result.event_id == "evt-1"
        assert result.source == "311"

    def test_payload_preserved(self) -> None:
        proc = EventProcessor()
        result = proc.process_event(_make_raw())
        assert result is not None
        assert result.payload["complaint_type"] == "Noise - Residential"


class TestEventProcessorDedup:
    def test_duplicate_rejected(self) -> None:
        proc = EventProcessor()
        proc.process_event(_make_raw("dup-1"))
        result = proc.process_event(_make_raw("dup-1"))
        assert result is None

    def test_different_ids_both_accepted(self) -> None:
        proc = EventProcessor()
        r1 = proc.process_event(_make_raw("id-a"))
        r2 = proc.process_event(_make_raw("id-b"))
        assert r1 is not None
        assert r2 is not None

    def test_dedup_window_eviction(self) -> None:
        proc = EventProcessor(dedup_window=timedelta(milliseconds=1))
        proc.process_event(_make_raw("evict-1"))
        # Force eviction by advancing received_at of the stored entry
        import time; time.sleep(0.01)
        # Trigger eviction via a new event
        proc.process_event(_make_raw("trigger"))
        # Now the original ID should be gone from the window
        result = proc.process_event(_make_raw("evict-1"))
        assert result is not None


class TestEventProcessorMalformed:
    def test_missing_event_id_rejected(self) -> None:
        raw = {"source": "311", "timestamp": datetime.now(tz=timezone.utc).isoformat()}
        assert EventProcessor().process_event(raw) is None

    def test_missing_timestamp_rejected(self) -> None:
        raw = {"event_id": "x", "source": "311"}
        assert EventProcessor().process_event(raw) is None

    def test_invalid_timestamp_format_rejected(self) -> None:
        raw = {"event_id": "x", "source": "311", "timestamp": "not-a-date"}
        assert EventProcessor().process_event(raw) is None


class TestEventProcessorLate:
    def test_late_event_still_accepted(self) -> None:
        proc = EventProcessor()
        # 10 minutes late — past the default 5-minute watermark
        result = proc.process_event(_make_raw("late-1", offset_seconds=600))
        assert result is not None
        assert result.is_late is True

    def test_on_time_event_not_late(self) -> None:
        proc = EventProcessor()
        result = proc.process_event(_make_raw("fresh-1", offset_seconds=10))
        assert result is not None
        assert result.is_late is False


class TestEventProcessorProcessMethod:
    def test_process_raises(self, tmp_path) -> None:
        from pathlib import Path
        proc = EventProcessor()
        with pytest.raises(NotImplementedError):
            proc.process(tmp_path / "src", tmp_path / "dst")

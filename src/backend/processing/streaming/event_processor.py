"""
Streaming Event Processor

Consumes events from Kafka, validates, deduplicates, and applies
windowed aggregations (per assignment spec section 4.2).
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor


@dataclass
class EventEnvelope:
    """Parsed and validated event envelope."""

    event_id: str
    source: str
    timestamp: datetime
    payload: dict
    received_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))

    @property
    def is_late(self, watermark_delay: timedelta = timedelta(minutes=5)) -> bool:
        """True if the event arrived later than the allowed watermark delay."""
        return self.received_at - self.timestamp > watermark_delay


class EventProcessor(BaseProcessor):
    """
    Stateful stream processor: parses envelopes, deduplicates within a window,
    detects out-of-order events, and produces aggregated metrics.
    """

    def __init__(
        self,
        dedup_window: timedelta = timedelta(minutes=10),
        window_size: timedelta = timedelta(minutes=5),
    ) -> None:
        self._seen_ids: dict[str, datetime] = {}
        self._dedup_window = dedup_window
        self._window_size = window_size
        self._latest_timestamp_by_source: dict[str, datetime] = {}
        self._metrics: dict[tuple[datetime, str, str], dict[str, int]] = {}

    @property
    def processor_name(self) -> str:
        return "event_processor"

    def process(self, source: Path, destination: Path) -> int:
        """Process a batch of events from source. Returns count of accepted events."""
        raise NotImplementedError(
            "EventProcessor is designed for streaming use; call process_event() directly."
        )

    def process_event(self, raw: dict) -> EventEnvelope | None:
        """
        Parse, validate, and deduplicate a single raw event.

        Returns the envelope if accepted, None if rejected (duplicate / malformed).
        """
        envelope = self._parse_envelope(raw)
        if envelope is None:
            return None

        if self._is_duplicate(envelope):
            self._record_metric(envelope, duplicate=True)
            logger.debug("[event_processor] Duplicate event dropped: {}", envelope.event_id)
            return None

        out_of_order = self._is_out_of_order(envelope)

        if envelope.is_late:
            logger.warning("[event_processor] Late event received: {}", envelope.event_id)

        if out_of_order:
            logger.warning("[event_processor] Out-of-order event received: {}", envelope.event_id)

        self._record_metric(envelope, late=envelope.is_late, out_of_order=out_of_order)

        self._record_seen(envelope)
        self._update_latest_timestamp(envelope)
        return envelope

    def metrics_dataframe(self) -> pd.DataFrame:
        """Return current windowed metrics as a DataFrame."""
        rows: list[dict] = []
        for (window_start, source, subject), counters in sorted(
            self._metrics.items(),
            key=lambda item: (item[0][0], item[0][1], item[0][2]),
        ):
            event_count = counters["event_count"]
            late_count = counters["late_event_count"]
            duplicate_count = counters["duplicate_event_count"]
            out_of_order_count = counters["out_of_order_event_count"]
            rows.append(
                {
                    "window_start_utc": window_start,
                    "window_end_utc": window_start + self._window_size,
                    "source": source,
                    "subject": subject,
                    "event_count": event_count,
                    "late_event_count": late_count,
                    "duplicate_event_count": duplicate_count,
                    "out_of_order_event_count": out_of_order_count,
                    "late_event_ratio": (late_count / event_count) if event_count > 0 else 0.0,
                }
            )
        return pd.DataFrame(rows)

    def reset_metrics(self) -> None:
        """Clear current aggregation windows while keeping dedup state."""
        self._metrics.clear()

    # ── Private helpers ───────────────────────────────────────────────────────

    def _parse_envelope(self, raw: dict) -> EventEnvelope | None:
        try:
            return EventEnvelope(
                event_id=str(raw["event_id"]),
                source=str(raw["source"]),
                timestamp=datetime.fromisoformat(raw["timestamp"]),
                payload=raw.get("payload", {}),
            )
        except (KeyError, ValueError) as exc:
            logger.warning("[event_processor] Malformed event rejected: {}", exc)
            return None

    def _is_duplicate(self, envelope: EventEnvelope) -> bool:
        return envelope.event_id in self._seen_ids

    def _record_seen(self, envelope: EventEnvelope) -> None:
        self._seen_ids[envelope.event_id] = envelope.received_at
        self._evict_old_ids()

    def _evict_old_ids(self) -> None:
        cutoff = datetime.now(tz=UTC) - self._dedup_window
        self._seen_ids = {eid: ts for eid, ts in self._seen_ids.items() if ts > cutoff}

    def _is_out_of_order(self, envelope: EventEnvelope) -> bool:
        latest = self._latest_timestamp_by_source.get(envelope.source)
        return latest is not None and envelope.timestamp < latest

    def _update_latest_timestamp(self, envelope: EventEnvelope) -> None:
        current = self._latest_timestamp_by_source.get(envelope.source)
        if current is None or envelope.timestamp > current:
            self._latest_timestamp_by_source[envelope.source] = envelope.timestamp

    def _record_metric(
        self,
        envelope: EventEnvelope,
        late: bool = False,
        duplicate: bool = False,
        out_of_order: bool = False,
    ) -> None:
        subject = self._extract_subject(envelope)
        window_start = self._window_start(envelope.timestamp)
        key = (window_start, envelope.source, subject)
        counters = self._metrics.setdefault(
            key,
            {
                "event_count": 0,
                "late_event_count": 0,
                "duplicate_event_count": 0,
                "out_of_order_event_count": 0,
            },
        )

        if duplicate:
            counters["duplicate_event_count"] += 1
            return

        counters["event_count"] += 1
        if late:
            counters["late_event_count"] += 1
        if out_of_order:
            counters["out_of_order_event_count"] += 1

    def _extract_subject(self, envelope: EventEnvelope) -> str:
        payload = envelope.payload
        for field in ["complaint_type", "event_type", "event_name", "descriptor"]:
            value = payload.get(field)
            if value:
                return str(value)
        return "all"

    def _window_start(self, timestamp: datetime) -> datetime:
        ts_utc = timestamp.astimezone(UTC)
        window_seconds = max(int(self._window_size.total_seconds()), 60)
        bucket_start_seconds = int(ts_utc.timestamp()) // window_seconds * window_seconds
        return datetime.fromtimestamp(bucket_start_seconds, tz=UTC)

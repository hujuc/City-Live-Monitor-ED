"""
Streaming Event Processor

Consumes events from Kafka, validates, deduplicates, and applies
windowed aggregations (per assignment spec section 4.2).
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from backend.core.processor import BaseProcessor


@dataclass
class EventEnvelope:
    """Parsed and validated event envelope."""

    event_id: str
    source: str
    timestamp: datetime
    payload: dict
    received_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @property
    def is_late(self, watermark_delay: timedelta = timedelta(minutes=5)) -> bool:
        """True if the event arrived later than the allowed watermark delay."""
        return self.received_at - self.timestamp > watermark_delay


class EventProcessor(BaseProcessor):
    """
    Stateful stream processor: parses envelopes, deduplicates within a window,
    detects out-of-order events, and produces aggregated metrics.
    """

    def __init__(self, dedup_window: timedelta = timedelta(minutes=10)) -> None:
        self._seen_ids: dict[str, datetime] = {}
        self._dedup_window = dedup_window

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
            logger.debug("[event_processor] Duplicate event dropped: {}", envelope.event_id)
            return None

        if envelope.is_late:
            logger.warning("[event_processor] Late event received: {}", envelope.event_id)

        self._record_seen(envelope)
        return envelope

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
        cutoff = datetime.now(tz=timezone.utc) - self._dedup_window
        self._seen_ids = {
            eid: ts for eid, ts in self._seen_ids.items() if ts > cutoff
        }

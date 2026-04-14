"""Gold layer: event schedule dimension table enriched with temporal features."""

from pathlib import Path

import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor
from backend.storage.gold import GoldStorage
from backend.storage.silver import SilverStorage

_TABLE = "event_schedule"


class GoldEventScheduleProcessor(BaseProcessor):
    """Builds a curated event schedule from all Silver event partitions."""

    def __init__(
        self,
        silver_storage: SilverStorage | None = None,
        gold_storage: GoldStorage | None = None,
    ) -> None:
        self._silver = silver_storage or SilverStorage()
        self._gold = gold_storage or GoldStorage()

    @property
    def processor_name(self) -> str:
        return "gold_event_schedule_processor"

    def process(self, source: Path, destination: Path) -> int:
        return self.build()

    def build(self) -> int:
        """Read all Silver events, enrich, deduplicate, write Gold table."""
        logger.info("[gold/event_schedule] Loading Silver events partitions…")
        df = self._load_silver_events()
        if df.empty:
            logger.warning("[gold/event_schedule] No Silver events data found — skipping")
            return 0

        logger.info("[gold/event_schedule] Loaded {:,} rows — enriching…", len(df))
        result = self._enrich(df)
        self._gold.write(result, _TABLE)
        return len(result)

    def _load_silver_events(self) -> pd.DataFrame:
        files = self._silver.list_files("events")
        if not files:
            return pd.DataFrame()
        parts = []
        for f in files:
            try:
                parts.append(pd.read_parquet(f))
            except Exception as exc:
                logger.warning("[gold/event_schedule] Skipping {}: {}", f.name, exc)
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        data = df.copy()

        # Cross-partition dedup (each Silver file is already deduped, but
        # multiple partitions can overlap for multi-month events)
        data = data.drop_duplicates(subset=["event_id"])

        data["start_datetime_utc"] = pd.to_datetime(data["start_datetime_utc"], utc=True)

        data["start_hour_of_day"] = data["start_datetime_utc"].dt.hour.astype("int8")
        data["start_day_of_week"] = data["start_datetime_utc"].dt.dayofweek.astype("int8")
        data["is_weekend_event"] = data["start_day_of_week"] >= 5

        if "end_datetime_utc" in data.columns:
            data["end_datetime_utc"] = pd.to_datetime(data["end_datetime_utc"], utc=True)
            end_hour = data["end_datetime_utc"].dt.hour
        else:
            end_hour = pd.Series(0, index=data.index)

        data["is_nightlife_event"] = (data["start_hour_of_day"] >= 20) | (end_hour >= 22)

        keep = [
            "event_id",
            "event_name",
            "event_type",
            "borough",
            "event_location",
            "start_datetime_utc",
            "end_datetime_utc",
            "event_duration_hours",
            "start_hour_of_day",
            "start_day_of_week",
            "is_weekend_event",
            "is_nightlife_event",
        ]
        available = [c for c in keep if c in data.columns]
        return data[available].sort_values("start_datetime_utc").reset_index(drop=True)

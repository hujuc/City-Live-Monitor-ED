"""Gold layer: noise complaint aggregations by borough × hour × day-of-week.

Streams partition-by-partition to keep memory bounded.
"""

from pathlib import Path

import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor
from backend.storage.gold import GoldStorage
from backend.storage.silver import SilverStorage

_TABLE = "noise_by_borough_hour"
_KEYS = ["borough", "complaint_type", "descriptor", "hour_of_day", "day_of_week"]
_KEEP_COLS = [
    "unique_key",
    "borough",
    "complaint_type",
    "descriptor",
    "created_datetime_utc",
    "resolution_duration_hours",
]


class GoldNoiseAggregationProcessor(BaseProcessor):
    """Aggregates Silver 311 noise complaints by borough × complaint type × hour × day."""

    def __init__(
        self,
        silver_storage: SilverStorage | None = None,
        gold_storage: GoldStorage | None = None,
    ) -> None:
        self._silver = silver_storage or SilverStorage()
        self._gold = gold_storage or GoldStorage()

    @property
    def processor_name(self) -> str:
        return "gold_noise_aggregation_processor"

    def process(self, source: Path, destination: Path) -> int:
        return self.build()

    def build(self) -> int:
        files = self._silver.list_files("noise311")
        if not files:
            logger.warning("[gold/noise] No Silver noise311 data found — skipping")
            return 0

        logger.info("[gold/noise] Aggregating {} Silver partition(s)…", len(files))
        partials: list[pd.DataFrame] = []
        for i, f in enumerate(files, 1):
            try:
                part = pd.read_parquet(f, columns=_KEEP_COLS)
                partial = self._partial_aggregate(part)
                partials.append(partial)
                logger.info(
                    "[gold/noise] Partition {}/{}: {:,} rows → {:,} groups",
                    i,
                    len(files),
                    len(part),
                    len(partial),
                )
                del part
            except Exception as exc:
                logger.warning("[gold/noise] Skipping {}: {}", f.name, exc)

        if not partials:
            return 0

        logger.info("[gold/noise] Combining partials…")
        result = self._finalize(pd.concat(partials, ignore_index=True))
        self._gold.write(result, _TABLE)
        return len(result)

    def _partial_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        data = df.copy()
        data["created_datetime_utc"] = pd.to_datetime(data["created_datetime_utc"], utc=True)
        data["hour_of_day"] = data["created_datetime_utc"].dt.hour.astype("int8")
        data["day_of_week"] = data["created_datetime_utc"].dt.dayofweek.astype("int8")

        return data.groupby(_KEYS, as_index=False, dropna=False).agg(
            complaint_count=("unique_key", "count"),
            sum_resolution=("resolution_duration_hours", "sum"),
            resolution_count=("resolution_duration_hours", "count"),
        )

    def _finalize(self, partials: pd.DataFrame) -> pd.DataFrame:
        combined = partials.groupby(_KEYS, as_index=False, dropna=False).agg(
            complaint_count=("complaint_count", "sum"),
            sum_resolution=("sum_resolution", "sum"),
            resolution_count=("resolution_count", "sum"),
        )
        # Avoid division by zero when no rows had resolution_duration
        combined["avg_resolution_hours"] = combined["sum_resolution"].where(
            combined["resolution_count"] > 0
        ) / combined["resolution_count"].replace(0, pd.NA)

        combined["is_weekend"] = combined["day_of_week"] >= 5

        col_order = [
            "borough",
            "complaint_type",
            "descriptor",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "complaint_count",
            "avg_resolution_hours",
        ]
        return (
            combined[col_order]
            .sort_values(["borough", "complaint_type", "day_of_week", "hour_of_day"])
            .reset_index(drop=True)
        )

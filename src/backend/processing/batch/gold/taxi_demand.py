"""Gold layer: taxi demand aggregated by zone × hour × day-of-week.

Uses streaming aggregation: each Silver partition is reduced to partial
(sum, count) statistics, then combined — bounded memory regardless of
total row count.
"""

from pathlib import Path

import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor
from backend.storage.gold import GoldStorage
from backend.storage.silver import SilverStorage
from config.settings import Config

_TABLE = "taxi_demand_by_zone"
_ZONE_LOOKUP_PATH = Config.PROJECT_ROOT / "data" / "reference" / "taxi_zone_lookup.csv"
_KEYS = ["PULocationID", "hour_of_day", "day_of_week"]
_KEEP_COLS = [
    *_KEYS,
    "fare_amount",
    "trip_distance",
    "tip_percentage",
    "trip_duration_capped_minutes",
    "is_rush_hour",
    "is_speed_outlier",
    "is_tip_outlier",
    "is_duration_outlier_iqr",
]


class GoldTaxiDemandProcessor(BaseProcessor):
    """Aggregates Silver TLC trips into demand metrics by zone × hour × day-of-week."""

    def __init__(
        self,
        silver_storage: SilverStorage | None = None,
        gold_storage: GoldStorage | None = None,
        zone_lookup_path: Path | None = None,
    ) -> None:
        self._silver = silver_storage or SilverStorage()
        self._gold = gold_storage or GoldStorage()
        self._zone_lookup_path = zone_lookup_path or _ZONE_LOOKUP_PATH

    @property
    def processor_name(self) -> str:
        return "gold_taxi_demand_processor"

    def process(self, source: Path, destination: Path) -> int:
        return self.build()

    def build(self) -> int:
        """Stream-aggregate Silver TLC partitions, combine, write Gold table."""
        files = self._silver.list_files("tlc")
        if not files:
            logger.warning("[gold/taxi_demand] No Silver TLC data found — skipping")
            return 0

        logger.info("[gold/taxi_demand] Aggregating {} Silver partition(s)…", len(files))
        partials: list[pd.DataFrame] = []
        for i, f in enumerate(files, 1):
            try:
                part = pd.read_parquet(f, columns=_KEEP_COLS)
                partial = self._partial_aggregate(part)
                partials.append(partial)
                logger.info(
                    "[gold/taxi_demand] Partition {}/{}: {:,} rows → {:,} groups",
                    i,
                    len(files),
                    len(part),
                    len(partial),
                )
                del part  # release memory before next iteration
            except Exception as exc:
                logger.warning("[gold/taxi_demand] Skipping {}: {}", f.name, exc)

        if not partials:
            logger.warning("[gold/taxi_demand] No partitions produced partials — skipping")
            return 0

        logger.info("[gold/taxi_demand] Combining partials…")
        result = self._finalize(pd.concat(partials, ignore_index=True))
        self._gold.write(result, _TABLE)
        return len(result)

    def _partial_aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Per-partition aggregation: returns sum/count per group key.

        Outliers are excluded here so the cross-partition combine operates
        only on clean rows.
        """
        clean = df[~df["is_speed_outlier"] & ~df["is_tip_outlier"] & ~df["is_duration_outlier_iqr"]]
        clean = clean.drop(
            columns=["is_speed_outlier", "is_tip_outlier", "is_duration_outlier_iqr"]
        )

        agg = clean.groupby(_KEYS, as_index=False).agg(
            trip_count=("fare_amount", "size"),
            sum_fare=("fare_amount", "sum"),
            sum_distance=("trip_distance", "sum"),
            sum_tip_pct=("tip_percentage", "sum"),
            sum_duration=("trip_duration_capped_minutes", "sum"),
            sum_rush_hour=("is_rush_hour", "sum"),
        )
        return agg

    def _finalize(self, partials: pd.DataFrame) -> pd.DataFrame:
        """Combine per-partition sums and counts into final means and totals."""
        combined = partials.groupby(_KEYS, as_index=False).agg(
            trip_count=("trip_count", "sum"),
            sum_fare=("sum_fare", "sum"),
            sum_distance=("sum_distance", "sum"),
            sum_tip_pct=("sum_tip_pct", "sum"),
            sum_duration=("sum_duration", "sum"),
            sum_rush_hour=("sum_rush_hour", "sum"),
        )

        combined["avg_fare_amount"] = combined["sum_fare"] / combined["trip_count"]
        combined["avg_trip_duration_minutes"] = combined["sum_duration"] / combined["trip_count"]
        combined["avg_tip_percentage"] = combined["sum_tip_pct"] / combined["trip_count"]
        combined["total_trip_distance"] = combined["sum_distance"]
        combined["pct_rush_hour"] = combined["sum_rush_hour"] / combined["trip_count"]
        combined["is_weekend"] = combined["day_of_week"] >= 5

        lookup = self._load_zone_lookup()
        if lookup is not None:
            combined = combined.merge(lookup, on="PULocationID", how="left")
            combined["borough"] = combined["borough"].fillna("Unknown")
            combined["zone_name"] = combined["zone_name"].fillna("Unknown zone")
        else:
            combined["borough"] = "Unknown"
            combined["zone_name"] = "Unknown zone"

        col_order = [
            "PULocationID",
            "borough",
            "zone_name",
            "hour_of_day",
            "day_of_week",
            "is_weekend",
            "trip_count",
            "avg_fare_amount",
            "avg_trip_duration_minutes",
            "avg_tip_percentage",
            "total_trip_distance",
            "pct_rush_hour",
        ]
        return (
            combined[col_order]
            .sort_values(["PULocationID", "day_of_week", "hour_of_day"])
            .reset_index(drop=True)
        )

    def _load_zone_lookup(self) -> pd.DataFrame | None:
        if not self._zone_lookup_path.exists():
            logger.warning(
                "[gold/taxi_demand] Zone lookup not found at {} — borough/zone_name omitted",
                self._zone_lookup_path,
            )
            return None
        lookup = pd.read_csv(self._zone_lookup_path)
        return lookup.rename(
            columns={"LocationID": "PULocationID", "Borough": "borough", "Zone": "zone_name"}
        )[["PULocationID", "borough", "zone_name"]]

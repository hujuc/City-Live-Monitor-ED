"""Batch processing for NYC TLC trips (Bronze -> Silver)."""

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor
from backend.storage.silver import SilverStorage


def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


@dataclass
class BatchRunStats:
    files_seen: int = 0
    files_processed: int = 0
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0


class TLCBatchProcessor(BaseProcessor):
    """Transforms TLC parquet files into cleaned, feature-rich Silver partitions."""

    def __init__(self, silver_storage: SilverStorage | None = None) -> None:
        self._silver = silver_storage or SilverStorage()

    @property
    def processor_name(self) -> str:
        return "tlc_batch_processor"

    def process(self, source: Path, destination: Path) -> int:
        """Process one Bronze parquet file and write partitioned Silver parquet files."""
        df = pd.read_parquet(source)
        if df.empty:
            logger.warning("[batch/tlc] Empty source file: {}", source)
            return 0

        cleaned = self._transform(df)
        if cleaned.empty:
            logger.warning("[batch/tlc] No valid rows after cleaning: {}", source)
            return 0

        source_tag = destination.as_posix().strip("/") or "tlc/yellow"
        stem = source.stem

        for (year, month), part in cleaned.groupby(["partition_year", "partition_month"]):
            out_name = f"{stem}_silver.parquet"
            payload = part.drop(columns=["partition_year", "partition_month"])
            self._silver.write(payload, source_tag, int(year), int(month), out_name)

        return int(len(cleaned))

    def process_bronze_tree(
        self,
        bronze_root: Path,
        destination: Path,
        pattern: str = "*.parquet",
        max_files: int | None = None,
    ) -> BatchRunStats:
        """Process all matching parquet files from a Bronze folder."""
        stats = BatchRunStats()
        files = sorted(bronze_root.rglob(pattern))
        if max_files is not None:
            files = files[:max_files]

        stats.files_seen = len(files)
        for src in files:
            try:
                rows_before = len(pd.read_parquet(src))
                rows_after = self.process(src, destination)
                stats.files_processed += 1
                stats.rows_read += rows_before
                stats.rows_written += rows_after
                stats.rows_dropped += max(0, rows_before - rows_after)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[batch/tlc] Failed processing {}: {}", src, exc)

        logger.info(
            "[batch/tlc] done: files={}/{} rows={} written={} dropped={}",
            stats.files_processed,
            stats.files_seen,
            stats.rows_read,
            stats.rows_written,
            stats.rows_dropped,
        )
        return stats

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply typing, quality filters, and feature engineering rules."""
        data = df.copy()

        pickup_col = _first_present(
            list(data.columns),
            ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"],
        )
        dropoff_col = _first_present(
            list(data.columns),
            ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"],
        )

        if pickup_col is None or dropoff_col is None:
            logger.warning("[batch/tlc] Missing pickup/dropoff datetime columns")
            return pd.DataFrame()

        data["pickup_datetime_utc"] = pd.to_datetime(data[pickup_col], errors="coerce", utc=True)
        data["dropoff_datetime_utc"] = pd.to_datetime(data[dropoff_col], errors="coerce", utc=True)

        numeric_cols = [
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "passenger_count",
        ]
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")

        mask = data["pickup_datetime_utc"].notna() & data["dropoff_datetime_utc"].notna()

        if "trip_distance" in data.columns:
            mask &= data["trip_distance"] > 0
        if "fare_amount" in data.columns:
            mask &= data["fare_amount"] >= 0
        if "tip_amount" in data.columns:
            mask &= data["tip_amount"] >= 0
        if "total_amount" in data.columns:
            mask &= data["total_amount"] >= 0

        duration_minutes = (
            data["dropoff_datetime_utc"] - data["pickup_datetime_utc"]
        ).dt.total_seconds() / 60.0
        mask &= duration_minutes > 0

        if {"pickup_latitude", "pickup_longitude"}.issubset(data.columns):
            mask &= data["pickup_latitude"].between(-90, 90)
            mask &= data["pickup_longitude"].between(-180, 180)
        if {"dropoff_latitude", "dropoff_longitude"}.issubset(data.columns):
            mask &= data["dropoff_latitude"].between(-90, 90)
            mask &= data["dropoff_longitude"].between(-180, 180)

        data = data.loc[mask].copy()
        if data.empty:
            return data

        data["trip_duration_minutes"] = (
            data["dropoff_datetime_utc"] - data["pickup_datetime_utc"]
        ).dt.total_seconds() / 60.0

        if "trip_distance" in data.columns:
            duration_hours = data["trip_duration_minutes"] / 60.0
            data["trip_speed_mph"] = data["trip_distance"] / duration_hours.replace(0, pd.NA)
        else:
            data["trip_speed_mph"] = pd.NA

        data["hour_of_day"] = data["pickup_datetime_utc"].dt.hour
        data["day_of_week"] = data["pickup_datetime_utc"].dt.dayofweek
        data["is_weekend"] = data["day_of_week"] >= 5
        data["is_rush_hour"] = data["hour_of_day"].isin([7, 8, 9, 16, 17, 18, 19])

        if {"tip_amount", "fare_amount"}.issubset(data.columns):
            data["tip_percentage"] = (
                data["tip_amount"] / data["fare_amount"].replace(0, pd.NA)
            ) * 100.0
        else:
            data["tip_percentage"] = pd.NA

        if {"total_amount", "trip_distance"}.issubset(data.columns):
            data["cost_per_mile"] = data["total_amount"] / data["trip_distance"].replace(0, pd.NA)
        else:
            data["cost_per_mile"] = pd.NA

        data["is_unusual_midnight_trip"] = data["hour_of_day"].between(0, 3)
        data["is_tip_outlier"] = data["tip_percentage"] > 50
        data["is_speed_outlier"] = data["trip_speed_mph"] > 80

        q1 = data["trip_duration_minutes"].quantile(0.25)
        q3 = data["trip_duration_minutes"].quantile(0.75)
        iqr = q3 - q1
        upper_iqr = q3 + 1.5 * iqr
        cap_95 = data["trip_duration_minutes"].quantile(0.95)
        data["is_duration_outlier_iqr"] = data["trip_duration_minutes"] > upper_iqr
        data["trip_duration_capped_minutes"] = data["trip_duration_minutes"].clip(upper=cap_95)

        data["partition_year"] = data["pickup_datetime_utc"].dt.year
        data["partition_month"] = data["pickup_datetime_utc"].dt.month
        return data

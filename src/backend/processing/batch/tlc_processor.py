"""Batch processing for NYC TLC trips (Bronze -> Silver)."""

import gc
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

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
class FilterRuleStat:
    rule: str
    rows_dropped: int


@dataclass
class BatchRunStats:
    files_seen: int = 0
    files_processed: int = 0
    rows_read: int = 0
    rows_written: int = 0
    rows_dropped: int = 0
    filter_breakdown: list[FilterRuleStat] = field(default_factory=list)

    def merge_filter_breakdown(self, other: list[FilterRuleStat]) -> None:
        """Accumulate per-rule drop counts across files."""
        existing = {s.rule: s for s in self.filter_breakdown}
        for stat in other:
            if stat.rule in existing:
                existing[stat.rule].rows_dropped += stat.rows_dropped
            else:
                new_stat = FilterRuleStat(rule=stat.rule, rows_dropped=stat.rows_dropped)
                self.filter_breakdown.append(new_stat)
                existing[stat.rule] = new_stat


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
        rows, _ = self._process_df(df, source, destination)
        return rows

    def _process_df(
        self, df: pd.DataFrame, source: Path, destination: Path
    ) -> tuple[int, list[FilterRuleStat]]:
        """Transform a pre-loaded DataFrame and write Silver partitions."""
        if df.empty:
            logger.warning("[batch/tlc] Empty source file: {}", source)
            return 0, []

        cleaned, breakdown = self._transform(df)
        if cleaned.empty:
            logger.warning("[batch/tlc] No valid rows after cleaning: {}", source)
            return 0, breakdown

        source_tag = destination.as_posix().strip("/") or "tlc/yellow"
        stem = source.stem

        for (year, month), part in cleaned.groupby(["partition_year", "partition_month"]):
            year_int = cast(int, year)
            month_int = cast(int, month)
            out_name = f"{stem}_silver.parquet"
            payload = part.drop(columns=["partition_year", "partition_month"])
            self._silver.write(payload, source_tag, year_int, month_int, out_name)

        return int(len(cleaned)), breakdown

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
                df = pd.read_parquet(src)
                rows_before = len(df)
                rows_after, breakdown = self._process_df(df, src, destination)
                stats.files_processed += 1
                stats.rows_read += rows_before
                stats.rows_written += rows_after
                stats.rows_dropped += max(0, rows_before - rows_after)
                stats.merge_filter_breakdown(breakdown)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[batch/tlc] Failed processing {}: {}", src, exc)
            finally:
                del df
                gc.collect()

        logger.info(
            "[batch/tlc] done: files={}/{} rows={} written={} dropped={}",
            stats.files_processed,
            stats.files_seen,
            stats.rows_read,
            stats.rows_written,
            stats.rows_dropped,
        )
        if stats.filter_breakdown:
            for rule_stat in stats.filter_breakdown:
                pct = rule_stat.rows_dropped / stats.rows_read * 100 if stats.rows_read else 0.0
                logger.info(
                    "[batch/tlc] filter '{}': {:,} rows dropped ({:.2f}%)",
                    rule_stat.rule,
                    rule_stat.rows_dropped,
                    pct,
                )
        return stats

    def _transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[FilterRuleStat]]:
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
            return pd.DataFrame(), []

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

        data, breakdown = self._apply_quality_filters(data)
        if data.empty:
            return data, breakdown

        return self._add_features(data), breakdown

    def _apply_quality_filters(
        self, data: pd.DataFrame
    ) -> tuple[pd.DataFrame, list[FilterRuleStat]]:
        """Apply each quality rule in sequence, tracking rows dropped per rule."""
        breakdown: list[FilterRuleStat] = []
        current = data

        def _apply(label: str, mask: pd.Series) -> pd.DataFrame:
            kept = current.loc[mask]
            dropped = len(current) - len(kept)
            if dropped:
                breakdown.append(FilterRuleStat(rule=label, rows_dropped=dropped))
            return kept

        rules: list[tuple[str, pd.Series]] = [
            (
                "invalid_datetime",
                current["pickup_datetime_utc"].notna() & current["dropoff_datetime_utc"].notna(),
            ),
            (
                "out_of_range_year",
                current["pickup_datetime_utc"].dt.year.between(2020, 2026),
            ),
        ]
        for label, mask in rules:
            current = _apply(label, mask)
            if current.empty:
                return current, breakdown

        for col, op, threshold, label in [
            ("trip_distance", "gt", 0, "zero_or_negative_distance"),
            ("fare_amount", "ge", 0, "negative_fare"),
            ("tip_amount", "ge", 0, "negative_tip"),
            ("total_amount", "ge", 0, "negative_total"),
            ("passenger_count", "ge", 1, "zero_or_missing_passengers"),
        ]:
            if col in current.columns:
                mask = getattr(current[col], op)(threshold)
                current = _apply(label, mask)
                if current.empty:
                    return current, breakdown

        duration_minutes = (
            current["dropoff_datetime_utc"] - current["pickup_datetime_utc"]
        ).dt.total_seconds() / 60.0
        current = _apply("non_positive_duration", duration_minutes > 0)
        if current.empty:
            return current, breakdown

        if {"pickup_latitude", "pickup_longitude"}.issubset(current.columns):
            mask = current["pickup_latitude"].between(-90, 90) & current[
                "pickup_longitude"
            ].between(-180, 180)
            current = _apply("invalid_pickup_coords", mask)
        if {"dropoff_latitude", "dropoff_longitude"}.issubset(current.columns):
            mask = current["dropoff_latitude"].between(-90, 90) & current[
                "dropoff_longitude"
            ].between(-180, 180)
            current = _apply("invalid_dropoff_coords", mask)

        return current, breakdown

    def _add_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add derived columns to an already-filtered DataFrame."""
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

        data["is_unusual_midnight_trip"] = data["hour_of_day"].between(0, 4)
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

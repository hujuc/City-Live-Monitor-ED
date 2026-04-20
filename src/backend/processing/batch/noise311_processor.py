"""Batch processing for NYC 311 Noise Complaints (Bronze NDJSON → Silver Parquet)."""

import json
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor
from backend.processing.batch.tlc_processor import BatchRunStats
from backend.storage.silver import SilverStorage

_SOURCE_TAG = "noise311"
# Bounding box for NYC (lat, lon)
_NYC_LAT = (40.40, 40.92)
_NYC_LON = (-74.26, -73.70)


def _read_ndjson(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    rows = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("[noise311/silver] Skipping malformed NDJSON line in {}", path.name)
    return rows


class Noise311SilverProcessor(BaseProcessor):
    """Transforms Bronze 311 Noise NDJSON snapshots into Silver Parquet."""

    def __init__(self, silver_storage: SilverStorage | None = None) -> None:
        self._silver = silver_storage or SilverStorage()

    @property
    def processor_name(self) -> str:
        return "noise311_silver_processor"

    def process(self, source: Path, destination: Path) -> int:
        """Process one Bronze NDJSON snapshot and write partitioned Silver Parquet."""
        rows = _read_ndjson(source)
        return self._process_rows(rows, source, destination)

    def _process_rows(self, rows: list[dict], source: Path, destination: Path) -> int:
        if not rows:
            logger.warning("[noise311/silver] Empty snapshot: {}", source)
            return 0

        df = pd.DataFrame(rows)
        cleaned = self._transform(df)
        if cleaned.empty:
            logger.warning("[noise311/silver] No valid rows after cleaning: {}", source)
            return 0

        source_tag = destination.as_posix().strip("/") or _SOURCE_TAG
        stem = source.stem
        for (year, month), part in cleaned.groupby(["partition_year", "partition_month"]):
            year_int = cast(int, year)
            month_int = cast(int, month)
            payload = part.drop(columns=["partition_year", "partition_month"])
            self._silver.write(
                payload, source_tag, year_int, month_int, f"{stem}_silver.parquet"
            )

        return len(cleaned)

    def process_bronze_tree(
        self,
        bronze_root: Path,
        destination: Path,
        pattern: str = "*.ndjson",
        max_files: int | None = None,
    ) -> BatchRunStats:
        """Process all NDJSON snapshots under a Bronze folder."""
        stats = BatchRunStats()
        files = sorted(bronze_root.rglob(pattern))
        if max_files is not None:
            files = files[:max_files]
        stats.files_seen = len(files)

        for src in files:
            try:
                rows = _read_ndjson(src)
                rows_before = len(rows)
                rows_after = self._process_rows(rows, src, destination)
                stats.files_processed += 1
                stats.rows_read += rows_before
                stats.rows_written += rows_after
                stats.rows_dropped += max(0, rows_before - rows_after)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[noise311/silver] Failed processing {}: {}", src, exc)

        logger.info(
            "[noise311/silver] done: files={}/{} rows={} written={} dropped={}",
            stats.files_processed,
            stats.files_seen,
            stats.rows_read,
            stats.rows_written,
            stats.rows_dropped,
        )
        return stats

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply validation, typing, coordinate filtering, and deduplication."""
        data = df.copy()

        if "created_date" not in data.columns:
            logger.warning("[noise311/silver] Missing 'created_date' column")
            return pd.DataFrame()

        data["created_datetime_utc"] = pd.to_datetime(
            data["created_date"], errors="coerce", utc=True
        )
        data = data.dropna(subset=["created_datetime_utc"])

        if "closed_date" in data.columns:
            data["closed_datetime_utc"] = pd.to_datetime(
                data["closed_date"], errors="coerce", utc=True
            )
            duration = (
                data["closed_datetime_utc"] - data["created_datetime_utc"]
            ).dt.total_seconds() / 3600.0
            data["resolution_duration_hours"] = duration.where(duration >= 0, other=np.nan)
        else:
            data["closed_datetime_utc"] = pd.NaT
            data["resolution_duration_hours"] = pd.NA

        for col in ["latitude", "longitude"]:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")

        data = self._filter_coordinates(data)

        if "unique_key" in data.columns:
            data = data.drop_duplicates(subset=["unique_key"])

        if "borough" in data.columns:
            data["borough"] = data["borough"].str.strip().str.title()

        data["partition_year"] = data["created_datetime_utc"].dt.year
        data["partition_month"] = data["created_datetime_utc"].dt.month
        return data.reset_index(drop=True)

    def _filter_coordinates(self, data: pd.DataFrame) -> pd.DataFrame:
        """Drop rows whose coordinates are present but fall outside NYC bounds."""
        if "latitude" not in data.columns or "longitude" not in data.columns:
            return data

        has_coords = data["latitude"].notna() & data["longitude"].notna()
        in_bounds = data["latitude"].between(*_NYC_LAT) & data["longitude"].between(*_NYC_LON)
        return data[~has_coords | in_bounds].copy()

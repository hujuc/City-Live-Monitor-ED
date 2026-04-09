"""Batch processing for NYC Permitted Events (Bronze NDJSON → Silver Parquet)."""

import hashlib
import json
from pathlib import Path

import pandas as pd
from loguru import logger

from backend.core.processor import BaseProcessor
from backend.processing.batch.tlc_processor import BatchRunStats
from backend.storage.silver import SilverStorage

_SOURCE_TAG = "events"
_REQUIRED_COLS = {"event_name", "start_date_time"}


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
            logger.warning("[events/silver] Skipping malformed NDJSON line in {}", path.name)
    return rows


def _derive_event_id(row: pd.Series) -> str:
    parts = [
        str(row.get("event_name", "") or ""),
        str(row.get("event_location", "") or ""),
        str(row.get("start_date_time", "") or ""),
        str(row.get("end_date_time", "") or ""),
    ]
    fingerprint = "nyc_permitted_events|" + "|".join(parts)
    return hashlib.sha256(fingerprint.encode()).hexdigest()[:32]


class PermittedEventsSilverProcessor(BaseProcessor):
    """Transforms Bronze Permitted Events NDJSON snapshots into Silver Parquet."""

    def __init__(self, silver_storage: SilverStorage | None = None) -> None:
        self._silver = silver_storage or SilverStorage()

    @property
    def processor_name(self) -> str:
        return "permitted_events_silver_processor"

    def process(self, source: Path, destination: Path) -> int:
        """Process one Bronze NDJSON snapshot and write partitioned Silver Parquet."""
        rows = _read_ndjson(source)
        return self._process_rows(rows, source, destination)

    def _process_rows(self, rows: list[dict], source: Path, destination: Path) -> int:
        if not rows:
            logger.warning("[events/silver] Empty snapshot: {}", source)
            return 0

        df = pd.DataFrame(rows)
        cleaned = self._transform(df)
        if cleaned.empty:
            logger.warning("[events/silver] No valid rows after cleaning: {}", source)
            return 0

        source_tag = destination.as_posix().strip("/") or _SOURCE_TAG
        for (year, month), part in cleaned.groupby(["partition_year", "partition_month"]):
            payload = part.drop(columns=["partition_year", "partition_month"])
            payload = self._merge_partition(source_tag, int(year), int(month), payload)
            self._silver.write(payload, source_tag, int(year), int(month), "events_silver.parquet")

        return len(cleaned)

    def _merge_partition(
        self, source_tag: str, year: int, month: int, new_data: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge new rows into the existing canonical partition file, dedup by event_id."""
        existing_path = self._silver.destination(source_tag, year, month, "events_silver.parquet")
        if not existing_path.exists():
            return new_data
        existing = pd.read_parquet(existing_path)
        merged = pd.concat([existing, new_data], ignore_index=True)
        before = len(merged)
        merged = merged.drop_duplicates(subset=["event_id"], keep="last")
        dupes = before - len(merged)
        if dupes:
            logger.debug("[events/silver] Deduped {} row(s) in {}/{:02d}", dupes, year, month)
        return merged.reset_index(drop=True)

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
                logger.exception("[events/silver] Failed processing {}: {}", src, exc)

        logger.info(
            "[events/silver] done: files={}/{} rows={} written={} dropped={}",
            stats.files_processed,
            stats.files_seen,
            stats.rows_read,
            stats.rows_written,
            stats.rows_dropped,
        )
        return stats

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply validation, typing, deduplication, and feature engineering."""
        data = df.copy()

        for col in _REQUIRED_COLS:
            if col not in data.columns:
                logger.warning("[events/silver] Missing required column: {}", col)
                return pd.DataFrame()

        data = data.dropna(subset=list(_REQUIRED_COLS))
        data = data[data["event_name"].str.strip().ne("")]

        data["start_datetime_utc"] = pd.to_datetime(
            data["start_date_time"], errors="coerce", utc=True
        )
        data = data.dropna(subset=["start_datetime_utc"])

        if "end_date_time" in data.columns:
            data["end_datetime_utc"] = pd.to_datetime(
                data["end_date_time"], errors="coerce", utc=True
            )
            duration = (
                data["end_datetime_utc"] - data["start_datetime_utc"]
            ).dt.total_seconds() / 3600.0
            data["event_duration_hours"] = duration.where(duration >= 0, other=pd.NA)
        else:
            data["end_datetime_utc"] = pd.NaT
            data["event_duration_hours"] = pd.NA

        for col in ["latitude", "longitude"]:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors="coerce")

        data["event_id"] = data.apply(_derive_event_id, axis=1)
        data = data.drop_duplicates(subset=["event_id"])

        borough_col = next((c for c in ["event_borough", "borough"] if c in data.columns), None)
        data["borough"] = data[borough_col].str.strip().str.title() if borough_col else pd.NA

        data["partition_year"] = data["start_datetime_utc"].dt.year
        data["partition_month"] = data["start_datetime_utc"].dt.month
        return data.reset_index(drop=True)

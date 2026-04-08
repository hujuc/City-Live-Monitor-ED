"""Tests for src/backend/processing/batch/events_processor.py"""

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.processing.batch.events_processor import PermittedEventsSilverProcessor
from backend.storage.silver import SilverStorage


def _write_ndjson(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")


def test_process_writes_silver_partition(tmp_path: Path) -> None:
    src = tmp_path / "bronze" / "snapshot_20260101T000000Z.ndjson"
    _write_ndjson(
        src,
        [
            {
                "event_name": "Jazz Night",
                "event_location": "Central Park",
                "start_date_time": "2025-06-15T20:00:00",
                "end_date_time": "2025-06-15T23:00:00",
                "event_borough": "Manhattan",
            }
        ],
    )

    silver = SilverStorage(root=tmp_path / "silver")
    proc = PermittedEventsSilverProcessor(silver_storage=silver)
    written = proc.process(src, destination=Path("events"))

    assert written == 1
    files = silver.list_files("events")
    assert len(files) == 1

    result = pd.read_parquet(files[0])
    assert len(result) == 1
    assert "start_datetime_utc" in result.columns
    assert "end_datetime_utc" in result.columns
    assert "event_duration_hours" in result.columns
    assert "event_id" in result.columns
    assert "borough" in result.columns
    assert result.iloc[0]["borough"] == "Manhattan"
    assert result.iloc[0]["event_duration_hours"] == pytest.approx(3.0)


def test_process_returns_zero_on_empty_file(tmp_path: Path) -> None:
    src = tmp_path / "empty.ndjson"
    src.write_text("", encoding="utf-8")

    proc = PermittedEventsSilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    assert proc.process(src, Path("events")) == 0


def test_rows_missing_start_date_are_dropped(tmp_path: Path) -> None:
    src = tmp_path / "snap.ndjson"
    _write_ndjson(
        src,
        [
            {"event_name": "A", "start_date_time": "2025-06-01T10:00:00"},
            {"event_name": "B"},  # missing start_date_time → dropped
        ],
    )

    proc = PermittedEventsSilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    assert proc.process(src, Path("events")) == 1


def test_deduplication_within_snapshot(tmp_path: Path) -> None:
    row = {
        "event_name": "Parade",
        "event_location": "5th Ave",
        "start_date_time": "2025-07-04T10:00:00",
        "end_date_time": "2025-07-04T14:00:00",
    }
    src = tmp_path / "snap.ndjson"
    _write_ndjson(src, [row, row])  # exact duplicate

    proc = PermittedEventsSilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    written = proc.process(src, Path("events"))

    assert written == 1


def test_negative_duration_set_to_null(tmp_path: Path) -> None:
    src = tmp_path / "snap.ndjson"
    _write_ndjson(
        src,
        [
            {
                "event_name": "Bad Times",
                "event_location": "Nowhere",
                "start_date_time": "2025-06-01T12:00:00",
                "end_date_time": "2025-06-01T10:00:00",  # end before start
            }
        ],
    )

    proc = PermittedEventsSilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    proc.process(src, Path("events"))

    result = pd.read_parquet(next((tmp_path / "silver").rglob("*.parquet")))
    assert pd.isna(result.iloc[0]["event_duration_hours"])


def test_process_bronze_tree_stats(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    for i in range(3):
        _write_ndjson(
            bronze / f"snap_{i}.ndjson",
            [{"event_name": f"Event {i}", "start_date_time": "2025-08-01T10:00:00"}],
        )

    proc = PermittedEventsSilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    stats = proc.process_bronze_tree(bronze_root=bronze, destination=Path("events"))

    assert stats.files_seen == 3
    assert stats.files_processed == 3
    assert stats.rows_read == 3
    assert stats.rows_written == 3
    assert stats.rows_dropped == 0

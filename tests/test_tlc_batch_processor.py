"""Tests for src/backend/processing/batch/tlc_processor.py"""

from pathlib import Path

import pandas as pd

from backend.processing.batch.tlc_processor import TLCBatchProcessor
from backend.storage.silver import SilverStorage


def test_process_bronze_tree_writes_clean_partition(tmp_path: Path) -> None:
    bronze_root = tmp_path / "bronze" / "tlc" / "yellow"
    source_file = bronze_root / "2025" / "yellow_tripdata_2025-01.parquet"
    source_file.parent.mkdir(parents=True, exist_ok=True)

    raw = pd.DataFrame(
        {
            "tpep_pickup_datetime": [
                "2025-01-01T01:00:00",
                "2025-01-01T02:00:00",
                "2025-01-01T03:00:00",
            ],
            "tpep_dropoff_datetime": [
                "2025-01-01T01:20:00",
                "2025-01-01T02:20:00",
                "2025-01-01T03:20:00",
            ],
            "trip_distance": [5.0, 0.0, 4.0],
            "fare_amount": [18.0, 12.0, -1.0],
            "tip_amount": [4.0, 1.0, 1.0],
            "total_amount": [25.0, 15.0, 3.0],
            "passenger_count": [1, 2, 1],
        }
    )
    raw.to_parquet(source_file, index=False)

    silver_root = tmp_path / "silver"
    silver = SilverStorage(root=silver_root)
    processor = TLCBatchProcessor(silver_storage=silver)

    stats = processor.process_bronze_tree(bronze_root=bronze_root, destination=Path("tlc/yellow"))

    assert stats.files_seen == 1
    assert stats.files_processed == 1
    assert stats.rows_read == 3
    assert stats.rows_written == 1
    assert stats.rows_dropped == 2

    out_files = silver.list_files("tlc/yellow")
    assert len(out_files) == 1

    result = pd.read_parquet(out_files[0])
    assert len(result) == 1
    assert "trip_duration_minutes" in result.columns
    assert "trip_speed_mph" in result.columns
    assert "tip_percentage" in result.columns
    assert "cost_per_mile" in result.columns
    assert "is_tip_outlier" in result.columns
    assert "is_speed_outlier" in result.columns
    assert "is_duration_outlier_iqr" in result.columns
    assert "trip_duration_capped_minutes" in result.columns
    assert "hour_of_day" in result.columns
    assert "day_of_week" in result.columns
    assert "is_weekend" in result.columns
    assert "is_rush_hour" in result.columns


def test_process_returns_zero_when_missing_datetime_columns(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pd.DataFrame({"trip_distance": [1.0], "fare_amount": [10.0]}).to_parquet(source, index=False)

    processor = TLCBatchProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    written = processor.process(source=source, destination=Path("tlc/yellow"))

    assert written == 0

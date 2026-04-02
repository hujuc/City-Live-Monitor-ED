"""Tests for src/backend/processing/batch/tlc_processor.py"""

from pathlib import Path

import pandas as pd
import pytest

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


def test_feature_engineering_values(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pd.DataFrame(
        {
            "tpep_pickup_datetime": ["2025-06-01T08:00:00"],
            "tpep_dropoff_datetime": ["2025-06-01T08:30:00"],
            "trip_distance": [10.0],
            "fare_amount": [20.0],
            "tip_amount": [5.0],
            "total_amount": [26.0],
            "passenger_count": [1],
        }
    ).to_parquet(source, index=False)

    proc = TLCBatchProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    proc.process(source=source, destination=Path("tlc/yellow"))

    result = pd.read_parquet(next((tmp_path / "silver").rglob("*.parquet")))
    assert len(result) == 1
    row = result.iloc[0]

    assert row["trip_duration_minutes"] == pytest.approx(30.0)
    assert row["trip_speed_mph"] == pytest.approx(20.0)  # 10 miles / 0.5 hours
    assert row["tip_percentage"] == pytest.approx(25.0)  # 5/20 * 100
    assert row["cost_per_mile"] == pytest.approx(2.6)  # 26/10
    assert row["hour_of_day"] == 8
    assert row["day_of_week"] == 6  # Sunday
    assert row["is_weekend"]
    assert row["is_rush_hour"]  # 08:00 is in morning rush [7, 8, 9]
    assert not row["is_unusual_midnight_trip"]
    assert not row["is_tip_outlier"]
    assert not row["is_speed_outlier"]


def test_outlier_flags_set_correctly(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pd.DataFrame(
        {
            "tpep_pickup_datetime": ["2025-06-01T02:00:00"],
            "tpep_dropoff_datetime": ["2025-06-01T03:00:00"],
            "trip_distance": [5.0],
            "fare_amount": [10.0],
            "tip_amount": [6.0],  # 60% tip → is_tip_outlier
            "total_amount": [17.0],
            "passenger_count": [1],
        }
    ).to_parquet(source, index=False)

    proc = TLCBatchProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    proc.process(source=source, destination=Path("tlc/yellow"))

    result = pd.read_parquet(next((tmp_path / "silver").rglob("*.parquet")))
    row = result.iloc[0]

    assert row["is_unusual_midnight_trip"]  # hour 2 → in [0..4]
    assert row["is_tip_outlier"]  # 60% > 50%


def test_quality_filter_removes_zero_passenger(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pd.DataFrame(
        {
            "tpep_pickup_datetime": ["2025-06-01T10:00:00", "2025-06-01T11:00:00"],
            "tpep_dropoff_datetime": ["2025-06-01T10:20:00", "2025-06-01T11:20:00"],
            "trip_distance": [3.0, 3.0],
            "fare_amount": [10.0, 10.0],
            "tip_amount": [1.0, 1.0],
            "total_amount": [12.0, 12.0],
            "passenger_count": [0, 1],  # first row has 0 passengers → dropped
        }
    ).to_parquet(source, index=False)

    proc = TLCBatchProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    written = proc.process(source=source, destination=Path("tlc/yellow"))

    assert written == 1

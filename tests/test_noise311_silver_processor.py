"""Tests for src/backend/processing/batch/noise311_processor.py"""

import json
from pathlib import Path

import pandas as pd
import pytest

from backend.processing.batch.noise311_processor import Noise311SilverProcessor
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
                "unique_key": "1234",
                "created_date": "2025-06-01T02:30:00",
                "closed_date": "2025-06-01T04:30:00",
                "complaint_type": "Noise - Residential",
                "descriptor": "Loud Music/Party",
                "borough": "BROOKLYN",
                "latitude": "40.6501",
                "longitude": "-73.9496",
                "status": "Closed",
            }
        ],
    )

    silver = SilverStorage(root=tmp_path / "silver")
    proc = Noise311SilverProcessor(silver_storage=silver)
    written = proc.process(src, destination=Path("noise311"))

    assert written == 1
    files = silver.list_files("noise311")
    assert len(files) == 1

    result = pd.read_parquet(files[0])
    row = result.iloc[0]
    assert "created_datetime_utc" in result.columns
    assert "closed_datetime_utc" in result.columns
    assert "resolution_duration_hours" in result.columns
    assert row["resolution_duration_hours"] == pytest.approx(2.0)
    assert row["borough"] == "Brooklyn"
    assert row["latitude"] == pytest.approx(40.6501)
    assert row["longitude"] == pytest.approx(-73.9496)


def test_process_returns_zero_on_empty_file(tmp_path: Path) -> None:
    src = tmp_path / "empty.ndjson"
    src.write_text("", encoding="utf-8")

    proc = Noise311SilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    assert proc.process(src, Path("noise311")) == 0


def test_out_of_bounds_coordinates_are_dropped(tmp_path: Path) -> None:
    src = tmp_path / "snap.ndjson"
    _write_ndjson(
        src,
        [
            {
                "unique_key": "good",
                "created_date": "2025-06-01T01:00:00",
                "complaint_type": "Noise - Street",
                "latitude": "40.7128",  # valid NYC
                "longitude": "-74.0060",
            },
            {
                "unique_key": "bad",
                "created_date": "2025-06-01T02:00:00",
                "complaint_type": "Noise - Street",
                "latitude": "51.5074",  # London
                "longitude": "-0.1278",
            },
        ],
    )

    proc = Noise311SilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    written = proc.process(src, Path("noise311"))

    assert written == 1


def test_rows_without_coordinates_are_kept(tmp_path: Path) -> None:
    src = tmp_path / "snap.ndjson"
    _write_ndjson(
        src,
        [
            {
                "unique_key": "no_coords",
                "created_date": "2025-06-01T01:00:00",
                "complaint_type": "Noise - Residential",
                # no latitude/longitude
            }
        ],
    )

    proc = Noise311SilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    written = proc.process(src, Path("noise311"))

    assert written == 1


def test_deduplication_by_unique_key(tmp_path: Path) -> None:
    row = {
        "unique_key": "ABC",
        "created_date": "2025-06-01T03:00:00",
        "complaint_type": "Noise - Residential",
    }
    src = tmp_path / "snap.ndjson"
    _write_ndjson(src, [row, row])

    proc = Noise311SilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    assert proc.process(src, Path("noise311")) == 1


def test_negative_resolution_duration_set_to_null(tmp_path: Path) -> None:
    src = tmp_path / "snap.ndjson"
    _write_ndjson(
        src,
        [
            {
                "unique_key": "X",
                "created_date": "2025-06-01T05:00:00",
                "closed_date": "2025-06-01T03:00:00",  # closed before created
                "complaint_type": "Noise - Vehicle",
            }
        ],
    )

    proc = Noise311SilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    proc.process(src, Path("noise311"))

    result = pd.read_parquet(next((tmp_path / "silver").rglob("*.parquet")))
    assert pd.isna(result.iloc[0]["resolution_duration_hours"])


def test_process_bronze_tree_stats(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    for i in range(2):
        _write_ndjson(
            bronze / f"snap_{i}.ndjson",
            [
                {
                    "unique_key": f"{i}a",
                    "created_date": "2025-09-01T00:00:00",
                    "complaint_type": "Noise - Residential",
                },
                {
                    "unique_key": f"{i}b",
                    "created_date": "2025-09-01T01:00:00",
                    "complaint_type": "Noise - Residential",
                },
            ],
        )

    proc = Noise311SilverProcessor(silver_storage=SilverStorage(root=tmp_path / "silver"))
    stats = proc.process_bronze_tree(bronze_root=bronze, destination=Path("noise311"))

    assert stats.files_seen == 2
    assert stats.files_processed == 2
    assert stats.rows_read == 4
    assert stats.rows_written == 4
    assert stats.rows_dropped == 0

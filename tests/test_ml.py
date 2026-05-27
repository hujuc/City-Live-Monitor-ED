"""Tests for ML dataset builder and classifier."""

from pathlib import Path

import pandas as pd
import pytest

from backend.ml.classifier import FEATURE_COLS, train_classifier
from backend.processing.batch.gold.ml_dataset_builder import (
    _extract_year_month,
    _in_date_range,
    build_ml_dataset,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_tlc_silver_parquet(tmp_path: Path, year: int, month: int, n: int = 50) -> Path:
    partition = tmp_path / "tlc" / "yellow" / f"year={year}" / f"month={month:02d}"
    partition.mkdir(parents=True)
    dates = pd.date_range(f"{year}-{month:02d}-01", periods=n, freq="h", tz="UTC")
    df = pd.DataFrame({"pickup_datetime_utc": dates})
    f = partition / "trips_silver.parquet"
    df.to_parquet(f)
    return f


def _make_noise_silver_parquet(tmp_path: Path, year: int, month: int, n: int = 30) -> Path:
    partition = tmp_path / "noise311" / f"year={year}" / f"month={month:02d}"
    partition.mkdir(parents=True)
    dates = pd.date_range(f"{year}-{month:02d}-01", periods=n, freq="h", tz="UTC")
    df = pd.DataFrame({"created_datetime_utc": dates})
    f = partition / "noise_silver.parquet"
    df.to_parquet(f)
    return f


def _make_events_silver_parquet(tmp_path: Path, year: int, month: int, n: int = 10) -> Path:
    partition = tmp_path / "events" / f"year={year}" / f"month={month:02d}"
    partition.mkdir(parents=True)
    dates = pd.date_range(f"{year}-{month:02d}-01", periods=n, freq="D")
    df = pd.DataFrame({"start_date_time": dates.strftime("%Y-%m-%dT%H:%M:%S.000")})
    f = partition / "events_silver.parquet"
    df.to_parquet(f)
    return f


# ── Unit tests ────────────────────────────────────────────────────────────────


def test_extract_year_month_valid(tmp_path: Path):
    p = tmp_path / "silver" / "tlc" / "year=2024" / "month=06" / "file.parquet"
    p.parent.mkdir(parents=True)
    p.touch()
    year, month = _extract_year_month(p)
    assert year == 2024
    assert month == 6


def test_extract_year_month_missing_parts(tmp_path: Path):
    p = tmp_path / "some" / "flat" / "file.parquet"
    p.parent.mkdir(parents=True)
    p.touch()
    year, month = _extract_year_month(p)
    assert year is None
    assert month is None


@pytest.mark.parametrize(
    "year,month,start_str,end_str,expected",
    [
        (2024, 6, "2024-06-01", "2024-12-31", True),  # overlaps
        (2024, 5, "2024-06-01", "2024-12-31", False),  # entirely before
        (2025, 1, "2024-06-01", "2024-12-31", False),  # entirely after
        (2024, 12, "2024-06-01", "2024-12-31", True),  # last month in range
    ],
)
def test_in_date_range(year, month, start_str, end_str, expected):
    start = pd.Timestamp(start_str, tz="UTC")
    end = pd.Timestamp(end_str, tz="UTC")
    assert _in_date_range(year, month, start, end) == expected


# ── Integration tests ─────────────────────────────────────────────────────────


def test_build_ml_dataset(tmp_path: Path):
    silver_root = tmp_path / "silver"
    _make_tlc_silver_parquet(silver_root, 2024, 6, n=100)
    _make_noise_silver_parquet(silver_root, 2024, 6, n=80)
    _make_events_silver_parquet(silver_root, 2024, 6, n=5)

    output = tmp_path / "ml_dataset.parquet"
    df = build_ml_dataset(silver_root, output, start_date="2024-06-01", end_date="2024-06-30")

    assert output.exists()
    assert len(df) > 0
    assert "noise_complaints" in df.columns
    assert "events_count" in df.columns
    assert "is_high_activity" in df.columns
    assert "day_of_week" in df.columns
    assert df["is_high_activity"].isin([0, 1]).all()
    assert (df["events_count"] >= 0).all()


def test_build_ml_dataset_events_zero_when_missing(tmp_path: Path):
    silver_root = tmp_path / "silver"
    _make_noise_silver_parquet(silver_root, 2024, 6, n=80)
    # No events Silver at all — events_count should be 0 everywhere

    output = tmp_path / "ml_dataset.parquet"
    df = build_ml_dataset(silver_root, output, start_date="2024-06-01", end_date="2024-06-30")

    assert len(df) > 0
    assert (df["events_count"] == 0).all()


def test_train_classifier(tmp_path: Path):
    import numpy as np

    n = 120
    rng = np.random.default_rng(42)
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=n),
            "noise_complaints": rng.integers(1_000, 5_000, n),
            "events_count": rng.integers(0, 50, n),
            "day_of_week": rng.integers(0, 7, n),
            "month": rng.integers(1, 13, n),
            "is_weekend": rng.integers(0, 2, n),
            "is_summer": rng.integers(0, 2, n),
            "is_holiday_season": rng.integers(0, 2, n),
            "is_high_activity": rng.integers(0, 2, n),
        }
    )
    dataset_path = tmp_path / "ml_dataset.parquet"
    df.to_parquet(dataset_path, index=False)

    model_path = tmp_path / "model.pkl"
    results = train_classifier(dataset_path, model_path)

    assert model_path.exists()
    assert 0.0 <= results["accuracy"] <= 1.0
    assert set(results["importances"].keys()) == set(FEATURE_COLS)
    assert results["train_size"] == int(n * 0.8)
    assert results["test_size"] == n - int(n * 0.8)


def test_train_classifier_missing_columns(tmp_path: Path):
    df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10), "taxi_trips": range(10)})
    dataset_path = tmp_path / "bad_dataset.parquet"
    df.to_parquet(dataset_path, index=False)

    with pytest.raises(ValueError, match="Missing columns"):
        train_classifier(dataset_path, tmp_path / "model.pkl")

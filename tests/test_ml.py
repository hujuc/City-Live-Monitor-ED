"""Tests for ML dataset builder and classifier."""

from pathlib import Path

import holidays as hol
import numpy as np
import pandas as pd
import pytest

from backend.ml.classifier import FEATURE_COLS, train_classifier
from backend.ml.predict import generate_predictions
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


def _make_ml_dataset(tmp_path: Path, n: int = 120) -> Path:
    """Build a synthetic ML parquet matching the schema produced by build_ml_dataset."""
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=n, tz="UTC")
    noise = rng.integers(1_000, 5_000, n).astype(float)
    noise_s = pd.Series(noise)
    noise_7d = noise_s.shift(1).rolling(7, min_periods=3).mean().round(1).values
    noise_30d = noise_s.shift(1).rolling(30, min_periods=15).mean().round(1).values
    noise_rank30d = noise_s.shift(1).rolling(30, min_periods=15).rank(pct=True).round(3).values
    month_s = pd.Series(dates.month)
    dow_vals = dates.dayofweek
    temp = pd.DataFrame({"day_of_week": dow_vals, "noise_complaints": noise})
    noise_dow = (
        temp.groupby("day_of_week")["noise_complaints"]
        .transform(lambda s: s.shift(1).expanding(min_periods=4).mean().round(1))
        .values
    )
    doy = pd.Series(dates.dayofyear)

    _hol = hol.US(years=[2023, 2024, 2025])
    _outdoor = {"Independence Day", "Memorial Day", "Labor Day"}
    _quiet = {"Thanksgiving", "Christmas Day"}
    _major_arr = sorted(
        {d for d, name in _hol.items() if name in _outdoor}
        | {d for d, name in _hol.items() if name in _quiet}
    )

    def _days_until(d):
        future = [h for h in _major_arr if h >= d]
        return int(min((future[0] - d).days, 7)) if future else 7

    def _days_since(d):
        past = [h for h in _major_arr if h < d]
        return int(min((d - past[-1]).days, 3)) if past else 3

    date_series = pd.Series(dates.date)
    days_until_vals = date_series.apply(_days_until).values
    days_since_vals = date_series.apply(_days_since).values

    df = pd.DataFrame(
        {
            "date": dates,
            "noise_complaints": noise,
            "taxi_trips": np.where(rng.random(n) < 0.1, np.nan, rng.integers(50_000, 200_000, n)),
            "events_count": rng.integers(0, 50, n),
            "day_of_week": dow_vals,
            "month": dates.month,
            "is_summer": month_s.isin([6, 7, 8]).astype(int).values,
            "is_holiday_season": month_s.isin([11, 12, 1]).astype(int).values,
            "is_holiday": rng.integers(0, 2, n),
            "is_outdoor_holiday": dates.date == pd.to_datetime("2024-07-04").date(),
            "is_quiet_holiday": np.zeros(n, dtype=int),
            "days_until_next_holiday": days_until_vals,
            "days_since_last_holiday": days_since_vals,
            "is_payday_week": pd.Series(dates.day)
            .isin([1, 2, 3, 14, 15, 16, 17])
            .astype(int)
            .values,
            "sin_doy": np.sin(2 * np.pi * doy / 365.25).round(4),
            "cos_doy": np.cos(2 * np.pi * doy / 365.25).round(4),
            "sin_doy_2": np.sin(4 * np.pi * doy / 365.25).round(4),
            "cos_doy_2": np.cos(4 * np.pi * doy / 365.25).round(4),
            "noise_7d_avg": noise_7d,
            "noise_30d_avg": noise_30d,
            "noise_percentile_rank_30d": noise_rank30d,
            "noise_dow_avg": noise_dow,
        }
    )
    path = tmp_path / "ml_dataset.parquet"
    df.to_parquet(path, index=False)
    return path


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
    assert "is_holiday" in df.columns
    assert "noise_7d_avg" in df.columns
    assert "noise_14d_avg" in df.columns
    assert "noise_30d_avg" in df.columns
    assert "noise_percentile_rank_30d" in df.columns
    assert "noise_dow_avg" in df.columns
    assert "is_outdoor_holiday" in df.columns
    assert "is_quiet_holiday" in df.columns
    assert "days_until_next_holiday" in df.columns
    assert "days_since_last_holiday" in df.columns
    assert "is_payday_week" in df.columns
    assert "sin_doy" in df.columns
    assert "cos_doy" in df.columns
    assert "taxi_trips" in df.columns
    assert "day_of_week" in df.columns
    # is_high_activity is NOT computed in the builder — computed in trainer only
    assert "is_high_activity" not in df.columns
    assert (df["events_count"] >= 0).all()


def test_build_ml_dataset_with_noise_counts(tmp_path: Path):
    silver_root = tmp_path / "silver"
    _make_noise_silver_parquet(silver_root, 2024, 6, n=80)

    hist = pd.DataFrame(
        {
            "date": pd.date_range("2023-06-01", periods=10, tz="UTC"),
            "noise_complaints": [200] * 10,
        }
    )
    noise_counts_path = tmp_path / "noise311_historical_counts.parquet"
    hist.to_parquet(noise_counts_path, index=False)

    output = tmp_path / "ml_dataset.parquet"
    df = build_ml_dataset(
        silver_root,
        output,
        start_date="2023-06-01",
        end_date="2024-06-30",
        noise_counts_path=noise_counts_path,
    )

    assert len(df) >= 10
    assert (df["noise_complaints"] >= 0).all()
    historical_rows = df[df["date"] < pd.Timestamp("2024-06-01", tz="UTC")]
    assert len(historical_rows) == 10
    assert (historical_rows["noise_complaints"] == 200).all()


def test_build_ml_dataset_events_zero_when_missing(tmp_path: Path):
    silver_root = tmp_path / "silver"
    _make_noise_silver_parquet(silver_root, 2024, 6, n=80)

    output = tmp_path / "ml_dataset.parquet"
    df = build_ml_dataset(silver_root, output, start_date="2024-06-01", end_date="2024-06-30")

    assert len(df) > 0
    assert (df["events_count"] == 0).all()


def test_train_classifier(tmp_path: Path):
    dataset_path = _make_ml_dataset(tmp_path, n=300)
    model_path = tmp_path / "model.pkl"
    results = train_classifier(dataset_path, model_path)

    assert model_path.exists()
    assert 0.0 <= results["accuracy"] <= 1.0
    assert 0.0 <= results["roc_auc"] <= 1.0
    assert set(results["importances"].keys()) == set(FEATURE_COLS)
    assert "baselines" in results
    assert "most_frequent" in results["baselines"]
    assert "cv_mean" in results
    assert "threshold" in results
    assert len(results["cv_scores"]) == 5
    assert all(0.0 <= s <= 1.0 for s in results["cv_scores"])


def test_train_classifier_target_not_from_test(tmp_path: Path):
    """Verify binary target threshold is computed from post-NaN-drop training rows only."""
    dataset_path = _make_ml_dataset(tmp_path, n=300)
    df = pd.read_parquet(dataset_path)
    df = df.sort_values("date").reset_index(drop=True)
    _rolling = [
        "noise_7d_avg",
        "noise_30d_avg",
        "noise_percentile_rank_30d",
        "noise_dow_avg",
    ]
    df_valid = df[df[_rolling].notna().all(axis=1)].reset_index(drop=True)
    split = int(len(df_valid) * 0.8)
    expected_threshold = float(df_valid["noise_complaints"].iloc[:split].median())

    model_path = tmp_path / "model.pkl"
    results = train_classifier(dataset_path, model_path)

    assert abs(results["threshold"] - expected_threshold) < 1.0


def test_train_classifier_missing_columns(tmp_path: Path):
    df = pd.DataFrame({"date": pd.date_range("2024-01-01", periods=10), "taxi_trips": range(10)})
    dataset_path = tmp_path / "bad_dataset.parquet"
    df.to_parquet(dataset_path, index=False)

    with pytest.raises(ValueError, match="Missing columns"):
        train_classifier(dataset_path, tmp_path / "model.pkl")


def test_generate_predictions_handles_date_objects(tmp_path: Path):
    dataset_path = _make_ml_dataset(tmp_path, n=300)
    model_path = tmp_path / "model.pkl"
    train_classifier(dataset_path, model_path)

    df = pd.read_parquet(dataset_path)
    df["date"] = df["date"].dt.date
    df.to_parquet(dataset_path, index=False)

    output_path = tmp_path / "daily_prediction.parquet"
    result = generate_predictions(dataset_path, model_path, output_path, horizon=7)

    assert output_path.exists()
    assert "today" in result
    assert len(result["forecast"]) == 7

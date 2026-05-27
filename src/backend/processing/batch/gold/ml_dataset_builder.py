"""Build ML training dataset from Silver TLC, Noise311, and Events layers."""

from pathlib import Path

import pandas as pd
from loguru import logger


def _extract_year_month(path: Path) -> tuple[int, int] | tuple[None, None]:
    parts = path.parts
    year_part = next((p for p in parts if p.startswith("year=")), None)
    month_part = next((p for p in parts if p.startswith("month=")), None)
    if not year_part or not month_part:
        return None, None
    return int(year_part.split("=")[1]), int(month_part.split("=")[1])


def _in_date_range(year: int, month: int, start: pd.Timestamp, end: pd.Timestamp) -> bool:
    file_start = pd.Timestamp(year=year, month=month, day=1, tz="UTC")
    file_end = file_start + pd.offsets.MonthEnd(1)
    return file_end >= start and file_start <= end


def _load_tlc_silver(tlc_root: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Aggregate Silver TLC trips per UTC date."""
    files = sorted(tlc_root.rglob("*.parquet"))
    parts = []
    for f in files:
        year, month = _extract_year_month(f)
        if year is None or not _in_date_range(year, month, start, end):
            continue
        df = pd.read_parquet(f, columns=["pickup_datetime_utc"])
        df = df[(df["pickup_datetime_utc"] >= start) & (df["pickup_datetime_utc"] <= end)]
        if df.empty:
            continue
        agg = (
            df.groupby(df["pickup_datetime_utc"].dt.normalize())
            .size()
            .rename("taxi_trips")
            .reset_index()
        )
        agg.columns = ["date", "taxi_trips"]
        parts.append(agg)

    if not parts:
        return pd.DataFrame(columns=["date", "taxi_trips"])

    result = pd.concat(parts).groupby("date")["taxi_trips"].sum().reset_index()
    logger.info("[ml_dataset] TLC: {} daily records", len(result))
    return result


def _load_noise_silver(noise_root: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Aggregate Silver Noise311 complaints per UTC date."""
    files = sorted(noise_root.rglob("*.parquet"))
    parts = []
    for f in files:
        year, month = _extract_year_month(f)
        if year is None or not _in_date_range(year, month, start, end):
            continue
        df = pd.read_parquet(f, columns=["created_datetime_utc"])
        df = df[(df["created_datetime_utc"] >= start) & (df["created_datetime_utc"] <= end)]
        if df.empty:
            continue
        agg = (
            df.groupby(df["created_datetime_utc"].dt.normalize())
            .size()
            .rename("noise_complaints")
            .reset_index()
        )
        agg.columns = ["date", "noise_complaints"]
        parts.append(agg)

    if not parts:
        return pd.DataFrame(columns=["date", "noise_complaints"])

    result = pd.concat(parts).groupby("date")["noise_complaints"].sum().reset_index()
    logger.info("[ml_dataset] Noise311: {} daily records", len(result))
    return result


def _load_events_silver(events_root: Path, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Aggregate permitted events per UTC date (by event start date)."""
    files = sorted(events_root.rglob("*.parquet"))
    parts = []
    for f in files:
        year, month = _extract_year_month(f)
        if year is None or not _in_date_range(year, month, start, end):
            continue
        df = pd.read_parquet(f, columns=["start_date_time"])
        # start_date_time is a string like "2026-09-01T06:00:00.000"
        dates = pd.to_datetime(df["start_date_time"], errors="coerce").dt.normalize()
        dates = dates.dropna()
        dates_utc = dates.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")
        dates_utc = dates_utc[(dates_utc >= start) & (dates_utc <= end)]
        if dates_utc.empty:
            continue
        agg = dates_utc.value_counts().rename("events_count").reset_index()
        agg.columns = ["date", "events_count"]
        parts.append(agg)

    if not parts:
        return pd.DataFrame(columns=["date", "events_count"])

    result = pd.concat(parts).groupby("date")["events_count"].sum().reset_index()
    logger.info("[ml_dataset] Events: {} daily records", len(result))
    return result


def build_ml_dataset(
    silver_root: Path,
    output_path: Path,
    start_date: str = "2024-06-01",
    end_date: str = "2026-05-27",
) -> pd.DataFrame:
    """Build an ML-ready daily dataset from Silver Noise311, Events, and (optionally) TLC.

    Target: is_high_noise_day — whether noise complaints exceed the median.
    This target is available across the full date range (Jun 2024 – May 2026).

    Events are a forward-looking feature (known before the night happens), making
    this model suitable for predicting activity before going out.
    """
    start = pd.Timestamp(start_date, tz="UTC")
    end = pd.Timestamp(end_date + "T23:59:59", tz="UTC")

    noise_df = _load_noise_silver(silver_root / "noise311", start, end)
    events_df = _load_events_silver(silver_root / "events", start, end)
    taxi_df = _load_tlc_silver(silver_root / "tlc/yellow", start, end)

    # Noise is the backbone — left-join everything onto it
    df = noise_df.copy()
    df = df.merge(events_df, on="date", how="left")
    df = df.merge(taxi_df, on="date", how="left")

    # Days with no events data get 0 (no permitted events known for that day)
    df["events_count"] = df["events_count"].fillna(0).astype(int)

    df = df.sort_values("date").reset_index(drop=True)

    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["is_summer"] = df["month"].isin([6, 7, 8]).astype(int)
    df["is_holiday_season"] = df["month"].isin([11, 12, 1]).astype(int)

    # Target: is this a high-activity night? (noise complaints above median)
    threshold = df["noise_complaints"].median()
    df["is_high_activity"] = (df["noise_complaints"] > threshold).astype(int)

    has_events = (df["events_count"] > 0).sum()
    has_taxi = df["taxi_trips"].notna().sum()
    logger.info(
        "[ml_dataset] {} daily records ({:.1f}% high-activity); "
        "days_with_events={} days_with_taxi={}; noise_threshold={:.0f}",
        len(df),
        df["is_high_activity"].mean() * 100,
        has_events,
        has_taxi,
        threshold,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("[ml_dataset] Saved to {}", output_path)
    return df

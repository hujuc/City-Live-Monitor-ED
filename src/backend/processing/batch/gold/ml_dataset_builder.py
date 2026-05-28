"""Build ML training dataset from Silver TLC, Noise311, and Events layers."""

from pathlib import Path

import holidays as hol
import numpy as np
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
        dates = pd.to_datetime(df["start_date_time"], errors="coerce", utc=True).dt.normalize()
        dates = dates.dropna()
        dates = dates[(dates >= start) & (dates <= end)]
        if dates.empty:
            continue
        agg = dates.value_counts().rename("events_count").reset_index()
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
    start_date: str = "2023-01-01",
    end_date: str = "2026-05-28",
    events_counts_path: Path | None = None,
    noise_counts_path: Path | None = None,
) -> pd.DataFrame:
    """Build an ML-ready daily dataset from Silver Noise311, Events, and TLC.

    The binary target (is_high_activity) is NOT pre-computed here — it must be
    derived inside the trainer using the training-set median only, to avoid
    label leakage from test-period noise levels into the threshold.

    Output columns:
      date, noise_complaints, taxi_trips, events_count,
      day_of_week, month, is_summer, is_holiday_season, is_holiday,
      is_outdoor_holiday, is_quiet_holiday,
      days_until_next_holiday, days_since_last_holiday, is_payday_week,
      sin_doy, cos_doy, sin_doy_2, cos_doy_2,
      noise_7d_avg, noise_14d_avg, noise_30d_avg, noise_percentile_rank_30d, noise_dow_avg

    Raw columns kept for the trainer:
      noise_complaints  — used to compute binary target on train set
      taxi_trips        — unimputed; trainer imputes from train median

    events_counts_path: optional pre-aggregated parquet [date (UTC), events_count]
      from backfill-events-daily-counts. Merged with (summed over) Silver events.
    noise_counts_path: optional pre-aggregated parquet [date (UTC), noise_complaints]
      from backfill-noise311-daily-counts. Merged with (summed over) Silver noise311.
    """
    start = pd.Timestamp(start_date, tz="UTC")
    end = pd.Timestamp(end_date + "T23:59:59", tz="UTC")

    noise_df = _load_noise_silver(silver_root / "noise311", start, end)

    if noise_counts_path is not None and noise_counts_path.exists():
        hist = pd.read_parquet(noise_counts_path)
        hist["date"] = pd.to_datetime(hist["date"], utc=True)
        hist = hist[(hist["date"] >= start) & (hist["date"] <= end)]
        if not hist.empty and not noise_df.empty:
            hist = hist[hist["date"] < noise_df["date"].min()]
        if not hist.empty:
            noise_df = (
                pd.concat([noise_df, hist[["date", "noise_complaints"]]], ignore_index=True)
                .groupby("date")["noise_complaints"]
                .sum()
                .reset_index()
            )
            logger.info("[ml_dataset] Merged historical noise counts: {} daily records", len(hist))

    events_df = _load_events_silver(silver_root / "events", start, end)

    if events_counts_path is not None and events_counts_path.exists():
        hist = pd.read_parquet(events_counts_path)
        hist["date"] = pd.to_datetime(hist["date"], utc=True)
        hist = hist[(hist["date"] >= start) & (hist["date"] <= end)]
        if not hist.empty and not events_df.empty:
            hist = hist[hist["date"] < events_df["date"].min()]
        if not hist.empty:
            events_df = (
                pd.concat([events_df, hist[["date", "events_count"]]], ignore_index=True)
                .groupby("date")["events_count"]
                .sum()
                .reset_index()
            )
            logger.info("[ml_dataset] Merged historical events counts: {} daily records", len(hist))

    taxi_df = _load_tlc_silver(silver_root / "tlc/yellow", start, end)

    df = noise_df.copy()
    df = df.merge(events_df, on="date", how="left")
    df = df.merge(taxi_df, on="date", how="left")

    df["events_count"] = df["events_count"].fillna(0).astype(int)
    df = df.sort_values("date").reset_index(drop=True)

    # ── Calendar features ──────────────────────────────────────────────────────
    df["day_of_week"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month
    df["is_summer"] = df["month"].isin([6, 7, 8]).astype(int)
    df["is_holiday_season"] = df["month"].isin([11, 12, 1]).astype(int)

    years = df["date"].dt.year.unique().tolist()
    us_holidays = hol.US(years=years)
    df["is_holiday"] = df["date"].dt.date.isin(us_holidays).astype(int)

    _outdoor = {"Independence Day", "Memorial Day", "Labor Day"}
    _quiet = {"Thanksgiving", "Christmas Day"}
    _outdoor_dates = {d for d, name in us_holidays.items() if name in _outdoor}
    _quiet_dates = {d for d, name in us_holidays.items() if name in _quiet}
    _major_arr = sorted(_outdoor_dates | _quiet_dates)
    df["is_outdoor_holiday"] = df["date"].dt.date.isin(_outdoor_dates).astype(int)
    df["is_quiet_holiday"] = df["date"].dt.date.isin(_quiet_dates).astype(int)

    def _days_until(d):
        future = [h for h in _major_arr if h >= d]
        return int(min((future[0] - d).days, 7)) if future else 7

    def _days_since(d):
        past = [h for h in _major_arr if h < d]
        return int(min((d - past[-1]).days, 3)) if past else 3

    df["days_until_next_holiday"] = df["date"].dt.date.apply(_days_until)
    df["days_since_last_holiday"] = df["date"].dt.date.apply(_days_since)
    df["is_payday_week"] = df["date"].dt.day.isin([1, 2, 3, 14, 15, 16, 17]).astype(int)

    doy = df["date"].dt.dayofyear
    df["sin_doy"] = np.sin(2 * np.pi * doy / 365.25).round(4)
    df["cos_doy"] = np.cos(2 * np.pi * doy / 365.25).round(4)
    df["sin_doy_2"] = np.sin(4 * np.pi * doy / 365.25).round(4)
    df["cos_doy_2"] = np.cos(4 * np.pi * doy / 365.25).round(4)

    # ── Noise rolling features (shift to avoid same-day leak) ──────────────────
    df["noise_7d_avg"] = df["noise_complaints"].shift(1).rolling(7, min_periods=3).mean().round(1)
    df["noise_14d_avg"] = df["noise_complaints"].shift(1).rolling(14, min_periods=7).mean().round(1)
    df["noise_30d_avg"] = (
        df["noise_complaints"].shift(1).rolling(30, min_periods=15).mean().round(1)
    )
    df["noise_percentile_rank_30d"] = (
        df["noise_complaints"].shift(1).rolling(30, min_periods=15).rank(pct=True).round(3)
    )
    df["noise_dow_avg"] = df.groupby("day_of_week")["noise_complaints"].transform(
        lambda s: s.shift(1).expanding(min_periods=4).mean().round(1)
    )
    # taxi_trips stays raw (NaN for 2026); trainer imputes from train median

    has_events = (df["events_count"] > 0).sum()
    has_taxi = df["taxi_trips"].notna().sum()
    has_holiday = df["is_holiday"].sum()
    logger.info(
        "[ml_dataset] {} daily records; days_with_events={} days_with_taxi={} holidays={}",
        len(df),
        has_events,
        has_taxi,
        has_holiday,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("[ml_dataset] Saved to {}", output_path)
    return df

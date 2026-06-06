"""Generate activity predictions from the trained classifier for all dates in the ML dataset."""

from datetime import date
from pathlib import Path

import holidays as hol
import joblib
import numpy as np
import pandas as pd
from loguru import logger

from backend.ml.classifier import FEATURE_COLS


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the same feature engineering as train_classifier (must stay in sync)."""
    df = df.sort_values("date").reset_index(drop=True)

    df["_events_log1p"] = np.log1p(df["events_count"])
    _is_weekend = (df["day_of_week"] >= 5).astype(int)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
    df["weekend_events"] = _is_weekend * df["_events_log1p"]
    df["friday_events"] = (df["day_of_week"] == 4).astype(int) * df["_events_log1p"]
    df["events_dow_avg"] = df.groupby("day_of_week")["_events_log1p"].transform(
        lambda s: s.shift(1).expanding(min_periods=4).mean().fillna(0.0)
    )

    _rolling_cols = ["noise_7d_avg", "noise_30d_avg", "noise_percentile_rank_30d", "noise_dow_avg"]
    valid = df[_rolling_cols].notna().all(axis=1)
    return df[valid].reset_index(drop=True)


def _build_future_rows(df: pd.DataFrame, horizon: int) -> pd.DataFrame:
    """Create placeholder rows for the next `horizon` days after the last date in df.

    Rolling noise features are carried forward from the last known values (per DOW for
    noise_dow_avg). Calendar features are computed fresh. events_count defaults to 0.
    """
    last_date = df["date"].max()
    future_dates = pd.date_range(
        start=last_date + pd.Timedelta(days=1),
        periods=horizon,
        freq="D",
        tz="UTC",
    )

    dow_noise_avg = df.groupby("day_of_week")["noise_dow_avg"].last()
    last_noise_7d = float(df["noise_7d_avg"].iloc[-1])
    last_noise_30d = float(df["noise_30d_avg"].iloc[-1])
    last_noise_pct = float(df["noise_percentile_rank_30d"].iloc[-1])

    years = list({ts.year for ts in future_dates})
    us_holidays = hol.US(years=years)
    _outdoor = {"Independence Day", "Memorial Day", "Labor Day"}
    _quiet = {"Thanksgiving", "Christmas Day"}
    _outdoor_dates = {d for d, name in us_holidays.items() if name in _outdoor}
    _quiet_dates = {d for d, name in us_holidays.items() if name in _quiet}
    _major_arr = sorted(_outdoor_dates | _quiet_dates)

    def _days_until(d: date) -> int:
        future_h = [h for h in _major_arr if h >= d]
        return int(min((future_h[0] - d).days, 7)) if future_h else 7

    rows = []
    for ts in future_dates:
        d = ts.date()
        dow = ts.dayofweek
        doy = ts.day_of_year
        month = ts.month
        rows.append(
            {
                "date": ts,
                "noise_complaints": np.nan,
                "events_count": 0,
                "taxi_trips": np.nan,
                "day_of_week": dow,
                "month": month,
                "sin_doy_2": round(np.sin(4 * np.pi * doy / 365.25), 4),
                "cos_doy": round(np.cos(2 * np.pi * doy / 365.25), 4),
                "days_until_next_holiday": _days_until(d),
                "noise_7d_avg": last_noise_7d,
                "noise_30d_avg": last_noise_30d,
                "noise_percentile_rank_30d": last_noise_pct,
                "noise_dow_avg": float(dow_noise_avg.get(dow, last_noise_7d)),
            }
        )

    return pd.DataFrame(rows)


def generate_predictions(
    dataset_path: Path,
    model_path: Path,
    output_path: Path,
    horizon: int = 7,
) -> dict:
    """Run inference on every date in the ML dataset plus the next `horizon` days.

    Writes a parquet with columns: date, predicted_label, activity_label, probability,
    is_forecast, run_at.  Returns a dict with keys 'today' (most recent actual date)
    and 'forecast' (list of dicts for future dates).
    """
    df = pd.read_parquet(dataset_path)
    df["date"] = pd.to_datetime(df["date"], utc=True)
    model = joblib.load(model_path)

    future_df: pd.DataFrame | None = None
    if horizon > 0:
        future_df = _build_future_rows(df, horizon)
        combined = pd.concat([df, future_df], ignore_index=True)
    else:
        combined = df.copy()

    combined = _build_features(combined)

    is_forecast_mask = (
        combined["date"].isin(future_df["date"])
        if future_df is not None
        else pd.Series(False, index=combined.index)
    )

    features = combined[FEATURE_COLS]
    proba = model.predict_proba(features)[:, 1]
    labels = model.predict(features).astype(int)

    out = pd.DataFrame(
        {
            "date": combined["date"],
            "predicted_label": labels,
            "activity_label": ["High Activity" if v else "Low Activity" for v in labels],
            "probability": proba.round(4),
            "is_forecast": is_forecast_mask.values,
            "run_at": pd.Timestamp.utcnow(),
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)
    logger.info(
        "[predict] {} predictions written to {} ({} forecast)", len(out), output_path, horizon
    )

    today = date.today()
    actual_rows = out[~out["is_forecast"]]
    today_rows = actual_rows[actual_rows["date"].dt.date == today]
    latest = today_rows.iloc[-1] if not today_rows.empty else actual_rows.iloc[-1]

    logger.info(
        "[predict] {} → {} (p={:.1%})",
        latest["date"].date() if hasattr(latest["date"], "date") else latest["date"],
        latest["activity_label"],
        latest["probability"],
    )

    forecast_rows = out[out["is_forecast"]].to_dict("records") if horizon > 0 else []
    return {"today": latest.to_dict(), "forecast": forecast_rows}

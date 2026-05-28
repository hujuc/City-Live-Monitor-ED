"""Generate activity predictions from the trained classifier for all dates in the ML dataset."""

from datetime import date
from pathlib import Path

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


def generate_predictions(
    dataset_path: Path,
    model_path: Path,
    output_path: Path,
) -> dict:
    """Run inference on every date in the ML dataset and write results to output_path.

    Returns a dict with the prediction for the most recent available date.
    """
    df = pd.read_parquet(dataset_path)
    model = joblib.load(model_path)

    df = _build_features(df)
    features = df[FEATURE_COLS]

    proba = model.predict_proba(features)[:, 1]
    labels = model.predict(features).astype(int)

    out = pd.DataFrame(
        {
            "date": df["date"],
            "predicted_label": labels,
            "activity_label": ["High Activity" if v else "Low Activity" for v in labels],
            "probability": proba.round(4),
            "run_at": pd.Timestamp.utcnow(),
        }
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(output_path, index=False)
    logger.info("[predict] {} predictions written to {}", len(out), output_path)

    today = date.today()
    today_rows = out[out["date"].dt.date == today]
    latest = today_rows.iloc[-1] if not today_rows.empty else out.iloc[-1]

    logger.info(
        "[predict] {} → {} (p={:.1%})",
        latest["date"].date() if hasattr(latest["date"], "date") else latest["date"],
        latest["activity_label"],
        latest["probability"],
    )
    return latest.to_dict()

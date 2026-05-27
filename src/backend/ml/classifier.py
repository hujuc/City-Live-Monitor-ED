"""Train and evaluate a Random Forest classifier for NYC nightlife activity."""

from pathlib import Path

import joblib
import pandas as pd
from loguru import logger
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# Forward-looking features: all known before the night starts
FEATURE_COLS = [
    "day_of_week",
    "month",
    "is_weekend",
    "is_summer",
    "is_holiday_season",
    "events_count",  # permits scheduled for that day — known in advance
]
TARGET_COL = "is_high_activity"


def train_classifier(dataset_path: Path, model_output: Path) -> dict:
    """Train a Random Forest on the ML dataset and save the model."""
    df = pd.read_parquet(dataset_path)

    missing = [c for c in FEATURE_COLS + [TARGET_COL] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataset: {missing}")

    features = df[FEATURE_COLS].copy()
    y = df[TARGET_COL]

    # Time-ordered train/test split — preserves temporal structure, no data leakage
    split = int(len(df) * 0.8)
    x_train, x_test = features.iloc[:split], features.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,
        min_samples_leaf=5,
        random_state=42,
        class_weight="balanced",
        n_jobs=-1,
    )
    model.fit(x_train, y_train)

    y_pred = model.predict(x_test)
    report = classification_report(y_test, y_pred, output_dict=True)

    importances = {
        feat: float(imp)
        for feat, imp in zip(FEATURE_COLS, model.feature_importances_, strict=False)
    }

    logger.info("[ml] Train size: {} | Test size: {}", len(x_train), len(x_test))
    logger.info("[ml] Test accuracy: {:.2f}%", report["accuracy"] * 100)
    logger.info(
        "[ml] Precision={:.3f} Recall={:.3f} F1={:.3f}",
        report["weighted avg"]["precision"],
        report["weighted avg"]["recall"],
        report["weighted avg"]["f1-score"],
    )
    for feat, imp in sorted(importances.items(), key=lambda x: -x[1]):
        logger.info("[ml] Feature importance  {:25s} {:.4f}", feat, imp)

    model_output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_output)
    logger.info("[ml] Model saved to {}", model_output)

    return {
        "accuracy": report["accuracy"],
        "precision": report["weighted avg"]["precision"],
        "recall": report["weighted avg"]["recall"],
        "f1": report["weighted avg"]["f1-score"],
        "importances": importances,
        "train_size": len(x_train),
        "test_size": len(x_test),
    }

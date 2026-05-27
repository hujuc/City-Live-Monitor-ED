"""Train and evaluate a Random Forest classifier for NYC nightlife activity."""

from pathlib import Path

import joblib
import pandas as pd
from loguru import logger
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

FEATURE_COLS = [
    "day_of_week",
    "month",
    "is_summer",
    "is_holiday_season",
    "is_holiday",
    "events_count",
    "noise_7d_avg",
    "taxi_trips_filled",
]
TARGET_COL = "is_high_activity"


def _make_target(noise_train: pd.Series, noise_full: pd.Series) -> pd.Series:
    """Compute binary target from training-set median only (avoids label leakage)."""
    threshold = float(noise_train.median())
    return (noise_full >= threshold).astype(int)


def _eval_baseline(
    name: str,
    clf,
    x_train: pd.DataFrame,
    y_train: pd.Series,
    x_test: pd.DataFrame,
    y_test: pd.Series,
) -> dict:
    clf.fit(x_train, y_train)
    y_pred = clf.predict(x_test)
    report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
    result = {
        "accuracy": report["accuracy"],
        "precision": report["weighted avg"]["precision"],
        "recall": report["weighted avg"]["recall"],
        "f1": report["weighted avg"]["f1-score"],
    }
    if hasattr(clf, "predict_proba"):
        proba = clf.predict_proba(x_test)[:, 1]
        result["roc_auc"] = float(roc_auc_score(y_test, proba))
    logger.info(
        "[ml] Baseline {:20s}  acc={:.2f}%  f1={:.3f}",
        name,
        result["accuracy"] * 100,
        result["f1"],
    )
    return result


def train_classifier(dataset_path: Path, model_output: Path) -> dict:
    """Train a Random Forest on the ML dataset and save the model."""
    df = pd.read_parquet(dataset_path)

    required = ["noise_complaints", "taxi_trips"] + [
        c for c in FEATURE_COLS if c not in ("taxi_trips_filled",)
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in dataset: {missing}")

    df = df.sort_values("date").reset_index(drop=True)

    # Time-ordered train/test split — preserves temporal structure, no data leakage
    split = int(len(df) * 0.8)

    # Impute taxi_trips from training-set median (avoids 2026 NaN leakage)
    train_taxi_median = float(df["taxi_trips"].iloc[:split].median())
    df["taxi_trips_filled"] = df["taxi_trips"].fillna(train_taxi_median)

    # Binary target computed from training-set median only
    y = _make_target(df["noise_complaints"].iloc[:split], df["noise_complaints"])
    threshold = float(df["noise_complaints"].iloc[:split].median())

    # Drop rows where noise_7d_avg is NaN (first few rows lack history)
    valid = df["noise_7d_avg"].notna()
    df = df[valid].reset_index(drop=True)
    y = y[valid].reset_index(drop=True)
    split = int(len(df) * 0.8)

    features = df[FEATURE_COLS].copy()
    x_train, x_test = features.iloc[:split], features.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]

    logger.info("[ml] Train size: {} | Test size: {}", len(x_train), len(x_test))
    logger.info("[ml] Activity threshold (train median): {:.0f} complaints/day", threshold)

    # ── Baselines ──────────────────────────────────────────────────────────────
    baselines = {
        "most_frequent": _eval_baseline(
            "most_frequent",
            DummyClassifier(strategy="most_frequent"),
            x_train,
            y_train,
            x_test,
            y_test,
        ),
        "stratified": _eval_baseline(
            "stratified",
            DummyClassifier(strategy="stratified", random_state=42),
            x_train,
            y_train,
            x_test,
            y_test,
        ),
        "logistic_regression": _eval_baseline(
            "logistic_regression",
            LogisticRegression(max_iter=500, random_state=42),
            StandardScaler().fit_transform(x_train),
            y_train,
            StandardScaler().fit_transform(x_test),
            y_test,
        ),
    }

    # ── TimeSeriesSplit cross-validation ──────────────────────────────────────
    tscv = TimeSeriesSplit(n_splits=5)
    cv_scores = []
    for fold, (tr_idx, val_idx) in enumerate(tscv.split(features)):
        xf_train, xf_val = features.iloc[tr_idx], features.iloc[val_idx]
        # Recompute target threshold per fold to avoid leakage
        fold_threshold = float(df["noise_complaints"].iloc[tr_idx].median())
        yf = (df["noise_complaints"] >= fold_threshold).astype(int)
        yf_train, yf_val = yf.iloc[tr_idx], yf.iloc[val_idx]

        clf_fold = RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            min_samples_leaf=5,
            random_state=42,
            class_weight="balanced",
            n_jobs=-1,
        )
        clf_fold.fit(xf_train, yf_train)
        proba = clf_fold.predict_proba(xf_val)[:, 1]
        score = float(roc_auc_score(yf_val, proba))
        cv_scores.append(score)
        logger.info("[ml] CV fold {}: ROC-AUC={:.3f}", fold + 1, score)

    cv_mean = float(pd.Series(cv_scores).mean())
    cv_std = float(pd.Series(cv_scores).std())
    logger.info("[ml] CV ROC-AUC: {:.3f} ± {:.3f}", cv_mean, cv_std)

    # ── Final model ───────────────────────────────────────────────────────────
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
    y_proba = model.predict_proba(x_test)[:, 1]
    report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
    roc_auc = float(roc_auc_score(y_test, y_proba))

    importances = {
        feat: float(imp)
        for feat, imp in zip(FEATURE_COLS, model.feature_importances_, strict=False)
    }

    logger.info("[ml] Test accuracy: {:.2f}%", report["accuracy"] * 100)
    logger.info("[ml] ROC-AUC: {:.3f}", roc_auc)
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
        "roc_auc": roc_auc,
        "cv_scores": cv_scores,
        "cv_mean": cv_mean,
        "cv_std": cv_std,
        "threshold": threshold,
        "importances": importances,
        "train_size": len(x_train),
        "test_size": len(x_test),
        "baselines": baselines,
    }

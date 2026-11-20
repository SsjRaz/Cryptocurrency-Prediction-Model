"""Reproducible, chronological baseline for next-day BTC direction.

This module is research-only: no order placement, portfolio claims or
implicit performance guarantees.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .features import FEATURE_COLUMNS, completed_daily, feature_frame, labeled_daily


def _model():
    # The scaler is fitted INSIDE each training fold, never on future test bars.
    return make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000, random_state=0))


def _scores(y_true: np.ndarray, p_up: np.ndarray) -> dict[str, float]:
    return {
        "accuracy": float(accuracy_score(y_true, p_up >= 0.5)),
        "brier": float(brier_score_loss(y_true, p_up)),
        "log_loss": float(log_loss(y_true, p_up, labels=[0, 1])),
    }


def walk_forward(frame: pd.DataFrame, *, n_splits: int = 5,
                 as_of: pd.Timestamp | None = None) -> dict:
    """Expanding-window testing with a one-row embargo and prior-only baseline.

    Each test sample is predicted only by a model fitted to historical rows.
    The target at day t depends on day t+1, so a gap of one sample prevents
    training with labels immediately next to the first test feature row.
    """
    if n_splits < 2:
        raise ValueError("n_splits must be at least 2")
    data = completed_daily(frame, as_of=as_of)
    labeled = labeled_daily(data)
    if len(labeled) < (n_splits + 1) * 20:
        raise ValueError("Need at least 20 labeled examples per split segment")

    x = labeled.loc[:, FEATURE_COLUMNS].to_numpy(dtype=float)
    y = labeled["target_up"].to_numpy(dtype=int)
    splitter = TimeSeriesSplit(n_splits=n_splits, gap=1)
    folds = []
    truth, model_probs, baseline_probs = [], [], []

    for fold_no, (train_idx, test_idx) in enumerate(splitter.split(x), start=1):
        if len(np.unique(y[train_idx])) != 2:
            raise ValueError(f"Training fold {fold_no} contains only one target class")

        model = _model()
        baseline = DummyClassifier(strategy="prior")
        model.fit(x[train_idx], y[train_idx])
        baseline.fit(x[train_idx], y[train_idx])
        pred = model.predict_proba(x[test_idx])[:, list(model.classes_).index(1)]
        naive = baseline.predict_proba(x[test_idx])[:, list(baseline.classes_).index(1)]
        observed = y[test_idx]

        truth.extend(observed.tolist())
        model_probs.extend(pred.tolist())
        baseline_probs.extend(naive.tolist())
        folds.append({
            "fold": fold_no,
            "train_rows": len(train_idx),
            "test_rows": len(test_idx),
            "train_end_utc": labeled.loc[train_idx[-1], "timestamp"].isoformat(),
            "test_start_utc": labeled.loc[test_idx[0], "timestamp"].isoformat(),
            "test_end_utc": labeled.loc[test_idx[-1], "timestamp"].isoformat(),
            "model": _scores(observed, pred),
            "prior_baseline": _scores(observed, naive),
        })

    return {
        "target": "next UTC daily candle close > current daily candle close",
        "validation": "expanding TimeSeriesSplit with one daily row embargo",
        "feature_columns": list(FEATURE_COLUMNS),
        "rows_labeled": int(len(labeled)),
        "folds": folds,
        "out_of_sample_rows": len(truth),
        "out_of_sample_model": _scores(np.asarray(truth), np.asarray(model_probs)),
        "out_of_sample_prior_baseline": _scores(np.asarray(truth), np.asarray(baseline_probs)),
    }


def latest_probability(frame: pd.DataFrame, *,
                       as_of: pd.Timestamp | None = None) -> dict:
    """Fit on available historical labels; predict the next COMPLETED close.

    The most recently closed candle supplies features, NOT a known target.
    This returns a probability for research, not a trading recommendation.
    """
    data = completed_daily(frame, as_of=as_of)
    labeled = labeled_daily(data)
    latest = feature_frame(data).iloc[-1]
    if latest.loc[list(FEATURE_COLUMNS)].isna().any():
        raise ValueError("Not enough completed candles for a current prediction")
    if len(labeled) < 60:
        raise ValueError("At least 60 labeled candles are required for training")

    x = labeled.loc[:, FEATURE_COLUMNS].to_numpy(dtype=float)
    y = labeled["target_up"].to_numpy(dtype=int)
    if len(np.unique(y)) != 2:
        raise ValueError("Training requires both UP and DOWN labels")
    model = _model()
    model.fit(x, y)
    current_features = latest.loc[list(FEATURE_COLUMNS)].to_numpy(dtype=float).reshape(1, -1)
    prob_up = float(model.predict_proba(current_features)[0, list(model.classes_).index(1)])
    open_utc = latest["timestamp"]
    return {
        "prediction_made_after_utc": (open_utc + pd.Timedelta(days=1)).isoformat(),
        "target_close_utc": (open_utc + pd.Timedelta(days=2)).isoformat(),
        "p_up": prob_up,
        "p_down": 1.0 - prob_up,
        "training_rows": len(labeled),
        "training_last_feature_utc": labeled.iloc[-1]["timestamp"].isoformat(),
        "disclaimer": "Research-only baseline; not investment advice or a validated edge.",
    }

"""Regression tests for historical integrity and leakage-safe research."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.baseline import latest_probability, walk_forward
from src.features import FEATURE_COLUMNS, completed_daily, feature_frame, labeled_daily, prepare_daily


@pytest.fixture
def daily():
    n = 420
    times = pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC")
    # Deterministic, varying both ways so time folds contain both classes.
    changes = 0.012 * np.sin(np.arange(n) * 0.31) + 0.007 * np.cos(np.arange(n) * 0.79)
    closes = 30000 * np.exp(np.cumsum(changes))
    opens = np.r_[30000.0, closes[:-1]]
    return pd.DataFrame({
        "timestamp": times,
        "open": opens,
        "high": np.maximum(opens, closes) * 1.004,
        "low": np.minimum(opens, closes) * 0.996,
        "close": closes,
        "volume": 100 + 15 * (1 + np.sin(np.arange(n) * 0.19)),
    })


AS_OF = pd.Timestamp("2026-01-01T00:00:00Z")


def test_normalization_sorts_without_changing_prices(daily):
    ordered = prepare_daily(daily.sample(frac=1.0, random_state=8))
    assert ordered["timestamp"].is_monotonic_increasing
    assert ordered["close"].tolist() == daily["close"].tolist()


@pytest.mark.parametrize("change", ["gap", "duplicate", "invalid_price", "invalid_high", "nan"])
def test_invalid_candles_fail_closed(daily, change):
    broken = daily.copy()
    if change == "gap":
        broken = broken.drop(index=31)
    elif change == "duplicate":
        broken.loc[31, "timestamp"] = broken.loc[30, "timestamp"]
    elif change == "invalid_price":
        broken.loc[31, "close"] = -1
    elif change == "invalid_high":
        broken.loc[31, "high"] = broken.loc[31, "low"] - 1
    else:
        broken.loc[31, "volume"] = np.nan
    with pytest.raises(ValueError):
        prepare_daily(broken)


def test_rejects_naive_as_of(daily):
    with pytest.raises(ValueError, match="timezone"):
        completed_daily(daily, as_of=pd.Timestamp("2025-01-01"))


def test_incomplete_daily_candle_is_excluded(daily):
    cutoff = daily.loc[200, "timestamp"] + pd.Timedelta(hours=12)
    completed = completed_daily(daily, as_of=cutoff)
    assert completed["timestamp"].iloc[-1] == daily.loc[199, "timestamp"]
    assert len(completed) == 200


def test_features_do_not_look_forward(daily):
    original = feature_frame(daily)
    modified = daily.copy()
    modified.loc[300:, "close"] *= 5.0
    modified.loc[300:, "high"] *= 5.0
    modified.loc[300:, "low"] *= 5.0
    modified.loc[300:, "open"] *= 5.0
    changed = feature_frame(modified)
    pd.testing.assert_frame_equal(
        original.loc[:299, list(FEATURE_COLUMNS)],
        changed.loc[:299, list(FEATURE_COLUMNS)],
    )


def test_labels_have_correct_alignment_and_no_unlabeled_last_row(daily):
    supervised = labeled_daily(daily)
    assert supervised["timestamp"].max() == daily["timestamp"].iloc[-2]
    assert len(supervised) > 350
    assert supervised["target_up"].isin([0, 1]).all()
    for row in supervised.itertuples():
        i = daily.index[daily["timestamp"] == row.timestamp][0]
        assert row.target_up == int(daily.loc[i + 1, "close"] > daily.loc[i, "close"])


def test_walk_forward_is_deterministic_and_chronological(daily):
    result = walk_forward(daily, n_splits=4, as_of=AS_OF)
    assert result == walk_forward(daily, n_splits=4, as_of=AS_OF)
    assert result["out_of_sample_rows"] > 200
    assert result["rows_labeled"] < len(daily)
    for fold in result["folds"]:
        assert fold["train_end_utc"] < fold["test_start_utc"]
        for method in ("model", "prior_baseline"):
            scores = fold[method]
            assert 0 <= scores["accuracy"] <= 1
            assert 0 <= scores["brier"] <= 1
            assert np.isfinite(scores["log_loss"])


def test_last_candle_prediction_uses_no_future_target(daily):
    result = latest_probability(daily, as_of=AS_OF)
    assert result["training_rows"] == len(labeled_daily(daily))
    assert result["training_last_feature_utc"] == daily["timestamp"].iloc[-2].isoformat()
    assert result["prediction_made_after_utc"] == (daily["timestamp"].iloc[-1] + pd.Timedelta(days=1)).isoformat()
    assert result["target_close_utc"] == (daily["timestamp"].iloc[-1] + pd.Timedelta(days=2)).isoformat()
    assert 0 <= result["p_up"] <= 1
    assert result["p_down"] == pytest.approx(1 - result["p_up"])


def test_not_enough_data_is_rejected(daily):
    with pytest.raises(ValueError):
        latest_probability(daily.iloc[:30], as_of=AS_OF)
    with pytest.raises(ValueError):
        walk_forward(daily.iloc[:40], n_splits=4, as_of=AS_OF)

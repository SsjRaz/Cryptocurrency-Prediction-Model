"""Separate execution-aligned target and conservative daily-open observation timing."""
import copy

import numpy as np
import pandas as pd
import pytest

from src.execution import simulate
from src.features import FEATURE_COLUMNS, prepare_daily
from src.ledger import calibrate, forecast_ledger
from src.targets import CLOSE_TARGET, OPEN_TARGET, target_data


@pytest.fixture
def market():
    n = 500
    opens = 100 * np.exp(.02 * np.sin(np.arange(n) * .43))
    # Opposing close/open sequences expose accidental old-target reuse.
    closes = 100 * np.exp(-.02 * np.sin(np.arange(n) * .43))
    return pd.DataFrame({"timestamp": pd.date_range("2023-01-01", periods=n, tz="UTC"),
                         "open": opens, "close": closes,
                         "high": np.maximum(opens, closes) * 1.01,
                         "low": np.minimum(opens, closes) * .99, "volume": 10.})


def run(market, **kwargs):
    return forecast_ledger(market, holdout_start="2024-01-01T00:00:00Z",
                           as_of="2025-01-01T00:00:00Z", target=OPEN_TARGET, **kwargs)


def test_target_audit_rows_and_synthetic_counterexample(market):
    aligned, purge = target_data(prepare_daily(market), target=OPEN_TARGET, lag_seconds=60)
    close, _ = target_data(prepare_daily(market), target=CLOSE_TARGET, lag_seconds=60)
    assert purge == 3
    for row in aligned.itertuples():
        i = market.index[market.timestamp == row.timestamp][0]
        assert row.target_up == int(market.open.iloc[i + 3] > market.open.iloc[i + 2])
        assert row.label_entry == row.timestamp + pd.Timedelta(days=2)
        assert row.label_exit == row.timestamp + pd.Timedelta(days=3)
        assert row.maturity == row.timestamp + pd.Timedelta(days=4, seconds=60)
    joined = aligned.merge(close, on="timestamp", suffixes=("_open", "_close"))
    assert (joined.target_up_open != joined.target_up_close).any()
    result = run(market)
    assert result == run(market)
    assert result["protocol"]["target_name"] == OPEN_TARGET
    for row in result["ledger"]:
        assert "label_close_utc" not in row
        assert row["target_up"] == int(row["label_exit_reference_open"] > row["label_entry_reference_open"])
        assert row["decision_utc"] < row["label_entry_utc"] < row["label_exit_utc"] < row["label_maturity_utc"]
        assert row["train_label_maturity_max_utc"] < row["freeze_utc"] <= row["decision_utc"]
        assert row["purged_rows"] == 3
        assert row["label_exit_utc"] < "2024-01-01T00:00:00+00:00"
        assert row["label_maturity_utc"] < "2024-01-01T00:01:01+00:00"
        if row["calibrated"]:
            assert row["calibration_label_maturity_max_utc"] < row["freeze_utc"]


def test_default_original_target_unchanged(market):
    default = forecast_ledger(market, holdout_start="2024-01-01T00:00:00Z", as_of="2025-01-01T00:00:00Z")
    explicit = forecast_ledger(market, holdout_start="2024-01-01T00:00:00Z", as_of="2025-01-01T00:00:00Z", target=CLOSE_TARGET)
    assert default == explicit
    assert all(row["purged_rows"] == 1 for row in default["ledger"])


def test_holdout_and_future_poisoning(market):
    original = run(market)
    changed = market.copy()
    changed.loc[changed.timestamp >= "2024-01-01", ["open", "high", "low", "close", "volume"]] = np.nan
    assert original == run(changed)
    changed = market.copy()
    changed.loc[300:, ["open", "high", "low", "close"]] *= 5
    later = run(changed)
    cutoff = market.timestamp.iloc[300].isoformat()
    before = lambda r: [row for row in r["ledger"] if row["label_maturity_utc"] < cutoff]
    assert before(original) == before(later)


def test_longer_horizon_purge_and_independent_oof_calibration(market):
    from src.baseline import _model
    from sklearn.linear_model import LogisticRegression
    data, _ = target_data(prepare_daily(market.loc[market.timestamp < "2024-01-01"]), target=OPEN_TARGET, lag_seconds=60)
    result = run(market)
    for fold in result["folds"]:
        freeze = pd.Timestamp(fold["freeze_utc"])
        train = data.loc[data.maturity < freeze]
        assert len(train) == fold["train_rows"]
        model = _model()
        model.fit(train.loc[:, FEATURE_COLUMNS].to_numpy(float), train.target_up.to_numpy(int))
        rows = [r for r in result["ledger"] if r["fold"] == fold["fold"]]
        test = data.loc[data.timestamp.isin(pd.to_datetime([r["feature_open_utc"] for r in rows]))]
        np.testing.assert_allclose(model.predict_proba(test.loc[:, FEATURE_COLUMNS].to_numpy(float))[:, 1], [r["logistic_raw"] for r in rows])
        prior = [r for r in result["ledger"] if r["fold"] < fold["fold"] and r["label_maturity_utc"] < fold["freeze_utc"]]
        assert len(prior) == fold["calibration_rows"]
        if fold["calibration_status"] == "fitted":
            p = np.clip([r["logistic_raw"] for r in prior], 1e-6, 1 - 1e-6)
            calibration = LogisticRegression(C=1, max_iter=2000, random_state=0).fit(np.log(p / (1 - p)).reshape(-1, 1), [r["target_up"] for r in prior])
            assert calibration.coef_[0, 0] == pytest.approx(fold["calibration_coefficient"])
    # A newly known exit open is still unavailable as a full daily OHLCV record.
    row = result["ledger"][0]
    tie = {**row, "label_maturity_utc": row["freeze_utc"]}
    _, _, eligible = calibrate([tie], freeze=pd.Timestamp(row["freeze_utc"]), min_rows=2)
    assert eligible == []


def test_delayed_lag_and_as_of(market):
    result = run(market, lag_seconds=86460)
    first = result["ledger"][0]
    assert pd.Timestamp(first["label_entry_utc"]) == pd.Timestamp(first["feature_open_utc"]) + pd.Timedelta(days=3)
    assert first["purged_rows"] == 4
    earlier = forecast_ledger(market, holdout_start="2024-01-01T00:00:00Z", as_of="2023-10-01T00:00:00Z", target=OPEN_TARGET)
    assert all(r["label_maturity_utc"] <= "2023-10-01T00:00:00+00:00" for r in earlier["ledger"])


def test_execution_alignment_and_scope_rejection(market):
    report = run(market)
    kwargs = dict(holdout_start="2024-01-01T00:00:00Z", as_of="2025-01-01T00:00:00Z",
                  window_start="2023-04-10T00:00:00Z", terminal_open="2023-12-31T00:00:00Z")
    result = simulate(market, report["ledger"], **kwargs)
    assert result["target_name"] == OPEN_TARGET
    assert result["horizon_limitation"]["accepted_at_or_after_target_exit"] == 0
    assert all(r["permitted_open_utc"] < r["target_exit_utc"] for r in result["orders"] if r["status"] == "accepted")
    broken = copy.deepcopy(report["ledger"])
    broken[0]["target_name"] = CLOSE_TARGET
    with pytest.raises(ValueError, match="Mixed target"):
        simulate(market, broken, **kwargs)
    broken = copy.deepcopy(report["ledger"])
    broken[0]["label_entry_utc"] = broken[0]["decision_utc"]
    with pytest.raises(ValueError, match="disagree"):
        simulate(market, broken, **kwargs)


def test_aligned_appended_data_forecasts_and_explicit_warmup(market):
    full = run(market)
    shorter = run(market.iloc[:280])
    lookup = {r["feature_open_utc"]: r for r in full["ledger"]}
    for row in shorter["ledger"]:
        for field in ("logistic_raw", "logistic_calibrated", "prior", "decision_utc", "label_maturity_utc", "freeze_utc", "calibration_rows"):
            assert row[field] == lookup[row["feature_open_utc"]][field]
    assert full["ledger"][0]["calibration_status"] == "warmup_insufficient_oof"
    assert full["counts"]["paired_calibrated"] > 0
    for population in ("all_oof", "paired_calibrated"):
        assert len({m["rows"] for m in full[population].values()}) == 1
    with pytest.raises(ValueError, match="Unknown"):
        forecast_ledger(market, holdout_start="2024-01-01T00:00:00Z", as_of="2025-01-01T00:00:00Z", target="unknown")

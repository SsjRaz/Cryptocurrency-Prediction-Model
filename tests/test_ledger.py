"""Synthetic logic tests; no market-performance evidence."""
import json

import numpy as np
import pandas as pd
import pytest

from src.ledger import calibrate, forecast_ledger


@pytest.fixture
def candles():
    n = 450
    close = 100 * np.exp(np.cumsum(.01 * np.sin(np.arange(n) * .53)))
    return pd.DataFrame({"timestamp": pd.date_range("2023-01-01", periods=n, tz="UTC"),
                         "open": close, "high": close * 1.02, "low": close * .98,
                         "close": close, "volume": 10 + np.arange(n) % 9})


def run(frame, **kwargs):
    return forecast_ledger(frame, holdout_start="2024-01-01T00:00:00Z",
                           as_of="2025-01-01T00:00:00Z", **kwargs)


def test_deterministic_ledger_and_strict_maturity(candles):
    result = run(candles)
    assert result == run(candles)
    json.dumps(result, allow_nan=False)
    assert result["counts"]["paired_calibrated"] > 0
    for row in result["ledger"]:
        assert row["train_label_maturity_max_utc"] < row["freeze_utc"] <= row["decision_utc"]
        assert row["candle_available_utc"] == row["decision_utc"]
        assert row["label_maturity_utc"] > row["decision_utc"]
        assert row["label_close_utc"] <= "2024-01-01T00:00:00+00:00"
        if row["calibrated"]:
            assert row["calibration_label_maturity_max_utc"] < row["freeze_utc"]
            assert row["calibration_feature_end_utc"] < row["test_start_utc"]
        else:
            assert row["logistic_calibrated"] == row["logistic_raw"]
    for population in ("all_oof", "paired_calibrated"):
        assert len({v["rows"] for v in result[population].values()}) == 1
        for diagnostic in result[population].values():
            assert sum(b["rows"] for b in diagnostic["reliability_bins"]) == diagnostic["rows"]
            assert diagnostic["scores"]["brier"] >= 0
    assert result["ledger"][0]["calibration_status"] == "warmup_insufficient_oof"


def test_holdout_poisoning(candles):
    original = run(candles)
    broken = candles.copy()
    broken.loc[broken.timestamp >= "2024-01-01", ["open", "high", "low", "close", "volume"]] = np.nan
    assert original == run(broken)


def test_future_mutation_does_not_change_earlier_forecasts(candles):
    original = run(candles)
    changed = candles.copy()
    changed.loc[300:, ["open", "high", "low", "close"]] *= 5
    mutated = run(changed)
    before = lambda result: [r for r in result["ledger"] if r["label_maturity_utc"] < candles.timestamp.iloc[300].isoformat()]
    assert before(original) == before(mutated)
    # Extending input cannot move earlier anchored fold boundaries/predictions.
    truncated = run(candles.iloc[:280])
    columns = ("feature_open_utc", "decision_utc", "freeze_utc", "logistic_raw", "logistic_calibrated", "calibration_rows", "target_up")
    lookup = {r["feature_open_utc"]: r for r in original["ledger"]}
    for row in truncated["ledger"]:
        assert {c: row[c] for c in columns} == {c: lookup[row["feature_open_utc"]][c] for c in columns}


def test_calibration_excludes_maturity_ties_and_duplicates():
    freeze = pd.Timestamp("2023-06-01T00:01:00Z")
    history = [{"feature_open_utc": str(i), "label_maturity_utc": (freeze - pd.Timedelta(days=i)).isoformat(),
                "raw_model_status": "fitted", "target_up": i % 2, "logistic_raw": .3 + .1 * (i % 2)} for i in range(5)]
    model, status, eligible = calibrate(history, freeze=freeze, min_rows=2)
    assert status == "fitted" and model is not None and len(eligible) == 4
    history[0]["target_up"] = 1 - history[0]["target_up"]
    other, _, _ = calibrate(history, freeze=freeze, min_rows=2)
    np.testing.assert_array_equal(model.coef_, other.coef_)
    with pytest.raises(ValueError, match="Duplicate"):
        calibrate(history + [history[-1]], freeze=freeze, min_rows=2)


def test_single_class_and_warmup_fallbacks(candles):
    growing = candles.copy()
    growing["close"] = np.arange(len(growing)) + 100.
    growing["open"] = growing.close
    growing["high"] = growing.close * 1.02
    growing["low"] = growing.close * .98
    result = run(growing)
    assert result["counts"]["paired_calibrated"] == 0
    assert all(r["raw_model_status"] == "single_class_prior_fallback" for r in result["ledger"])
    assert result["paired_calibrated"]["logistic_raw"]["scores"] is None
    history = [{"feature_open_utc": str(i), "label_maturity_utc": "2023-01-01T00:00:00Z",
                "raw_model_status": "fitted", "target_up": 1, "logistic_raw": .7} for i in range(5)]
    model, status, _ = calibrate(history, freeze=pd.Timestamp("2023-02-01T00:00:00Z"), min_rows=2)
    assert model is None and status == "warmup_single_class"
    assert run(candles, min_calibration=10000)["counts"]["paired_calibrated"] == 0


def test_same_fold_labels_cannot_change_frozen_calibrator(candles):
    original = run(candles)
    first = next(f for f in original["folds"] if f["calibration_status"] == "fitted")
    changed = candles.copy()
    # Change later candles in that test block, never its preceding OOF history.
    mask = changed.timestamp >= pd.Timestamp(first["test_start_utc"]) + pd.Timedelta(days=3)
    changed.loc[mask, ["open", "high", "low", "close"]] *= 2
    result = run(changed)
    before = [r for r in original["ledger"] if r["fold"] == first["fold"]][:2]
    after = [r for r in result["ledger"] if r["fold"] == first["fold"]][:2]
    assert before == after


@pytest.mark.parametrize("kwargs", [{"min_train": 1}, {"test_size": 0}, {"min_calibration": 1}, {"lag_seconds": -1}])
def test_invalid_protocol(candles, kwargs):
    with pytest.raises(ValueError):
        run(candles, **kwargs)


def test_independent_past_oof_calibration_fit(candles):
    from sklearn.linear_model import LogisticRegression
    from scipy.special import expit
    result = run(candles)
    for fold in result["folds"]:
        if fold["calibration_status"] != "fitted":
            continue
        past = [r for r in result["ledger"] if r["fold"] < fold["fold"]
                and r["label_maturity_utc"] < fold["freeze_utc"] and r["raw_model_status"] == "fitted"]
        assert len(past) == fold["calibration_rows"]
        p = np.clip([r["logistic_raw"] for r in past], 1e-6, 1 - 1e-6)
        model = LogisticRegression(C=1., random_state=0, max_iter=2000)
        model.fit(np.log(p / (1 - p)).reshape(-1, 1), [r["target_up"] for r in past])
        assert model.coef_[0, 0] == pytest.approx(fold["calibration_coefficient"])
        assert model.intercept_[0] == pytest.approx(fold["calibration_intercept"])
        for row in result["ledger"]:
            if row["fold"] == fold["fold"]:
                raw = np.clip(row["logistic_raw"], 1e-6, 1 - 1e-6)
                expected = expit(model.intercept_[0] + model.coef_[0, 0] * np.log(raw / (1 - raw)))
                assert row["logistic_calibrated"] == pytest.approx(expected)


def test_ledger_respects_as_of_lag(candles):
    result = forecast_ledger(candles, holdout_start="2024-01-01T00:00:00Z",
                             as_of="2023-10-01T00:00:00Z")
    assert all(r["label_maturity_utc"] <= "2023-10-01T00:00:00+00:00" for r in result["ledger"])
    assert result["ledger"][-1]["feature_open_utc"] == "2023-09-28T00:00:00+00:00"


def test_snapshot_ledger_cli_exclusive_output(candles, tmp_path, monkeypatch, capsys):
    from src import research
    manifest = {"availability_lag_seconds_assumed": 60, "sha256": "test-synthetic-digest"}
    monkeypatch.setattr(research, "load_snapshot", lambda directory: (candles, manifest))
    output = tmp_path / "ledger.json"
    monkeypatch.setattr("sys.argv", ["research", "ledger", "unused-synthetic-test", "--holdout-start",
                                   "2024-01-01T00:00:00Z", "--as-of", "2025-01-01T00:00:00Z",
                                   "--output", str(output)])
    research.main()
    result = json.loads(output.read_text())
    assert result["snapshot_sha256"] == manifest["sha256"]
    assert result["counts"]["oof"] > result["counts"]["paired_calibrated"] > 0
    original = output.read_bytes()
    with pytest.raises(FileExistsError):
        research.main()
    assert output.read_bytes() == original


def test_raw_logistic_matches_independent_purged_fit(candles):
    from src.baseline import _model
    from src.features import FEATURE_COLUMNS, labeled_daily
    result = run(candles)
    dev = candles.loc[candles.timestamp < "2024-01-01"]
    data = labeled_daily(dev)
    for fold in result["folds"]:
        freeze = pd.Timestamp(fold["freeze_utc"])
        train = data.loc[data.timestamp + pd.Timedelta(days=2, seconds=60) < freeze]
        assert len(train) == fold["train_rows"]
        model = _model()
        model.fit(train.loc[:, FEATURE_COLUMNS].to_numpy(dtype=float), train.target_up.to_numpy(dtype=int))
        rows = [r for r in result["ledger"] if r["fold"] == fold["fold"]]
        test = data.loc[data.timestamp.isin(pd.to_datetime([r["feature_open_utc"] for r in rows]))]
        predicted = model.predict_proba(test.loc[:, FEATURE_COLUMNS].to_numpy(dtype=float))[:, 1]
        np.testing.assert_allclose(predicted, [r["logistic_raw"] for r in rows], atol=1e-14)

"""Full frozen final workflow on generated synthetic prices only, never real holdout."""
import copy
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.final_runner import digest, frame_digest, freeze_model, predict_features, score_once
from src.protocol import protocol_receipt
from src.runtime import require_reference_runtime

try:
    require_reference_runtime()
except ValueError:
    pytestmark = pytest.mark.skip(reason="Final pipeline runs in reference hash-locked runtime; dedicated CI job covers it")


@pytest.fixture(scope="module")
def synthetic():
    times = pd.date_range("2023-01-01", "2025-12-30", tz="UTC")
    i = np.arange(len(times))
    opens = 100 * np.exp(.03 * np.sin(i * .43) + .01 * np.cos(i * .13))
    closes = opens * np.exp(.003 * np.sin(i * .71))
    frame = pd.DataFrame({"timestamp": times, "open": opens, "close": closes,
                          "high": np.maximum(opens, closes) * 1.01,
                          "low": np.minimum(opens, closes) * .99, "volume": 10. + i % 11})
    dev = frame.loc[frame.timestamp < "2025-01-01"].reset_index(drop=True)
    outcomes = frame.loc[frame.timestamp >= "2025-01-01"].reset_index(drop=True)
    features = outcomes.loc[outcomes.timestamp <= "2025-12-27"].reset_index(drop=True)
    receipt = protocol_receipt(json.loads(Path("protocols/preholdout.json").read_text()))
    bundle = freeze_model(dev, receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256=frame_digest(dev))
    sealed = predict_features(bundle, features, expected_freeze_sha256=digest(bundle))
    return dev, features, outcomes, receipt, bundle, sealed


def score(synthetic, tmp_path, **kwargs):
    _, _, outcomes, _, bundle, sealed = synthetic
    return score_once(bundle, sealed, outcomes, expected_freeze_sha256=digest(bundle),
                      expected_prediction_sha256=digest(sealed), expected_outcomes_sha256=frame_digest(outcomes),
                      state_directory=tmp_path, acknowledge_unblind=True, **kwargs)


def test_end_to_end_synthetic_counts_domains_and_single_local_attempt(synthetic, tmp_path):
    _, _, _, _, bundle, sealed = synthetic
    assert sealed["rows"] == 361
    assert sealed["first_feature"] == "2025-01-01T00:00:00+00:00"
    assert sealed["last_feature"] == "2025-12-27T00:00:00+00:00"
    assert all("target_up" not in row for row in sealed["predictions"])
    assert bundle["train_label_maturity_max_utc"] < bundle["freeze_utc"] < "2025-01-01T00:00:00+00:00"
    assert bundle["calibration_label_maturity_max_utc"] < bundle["freeze_utc"]
    result = score(synthetic, tmp_path)
    assert result["counts"] == {"predicted": 361, "scored": 361, "prior_support": 361, "calibration_fallback": 0}
    assert {d["rows"] for d in result["scores_identical_rows"].values()} == {361}
    assert result["domains"]["last_outcome_candle"] == "2025-12-30T00:00:00+00:00"
    costs = result["execution_diagnostics"]
    assert costs["first_open"] == "2025-01-03T00:00:00+00:00"
    assert costs["terminal_open"] == "2025-12-30T00:00:00+00:00"
    assert len(costs["scenarios"]) == 3
    for scenario in costs["scenarios"]:
        assert {len(s["equity"]) for s in scenario["strategies"].values()} == {362}
        assert scenario["strategies"]["no_trade"]["terminal_quote"] == 1000
        assert all(s["terminal_base"] == 0 for s in scenario["strategies"].values())
    assert result["uncertainty"]["scope"].startswith("frozen_final")
    completion = json.loads(next(tmp_path.glob("*.completed.json")).read_text())
    assert completion["canonical_report_sha256"] == digest(result)
    import hashlib
    report_file = Path(completion["report_path"])
    assert report_file.exists() and json.loads(report_file.read_text()) == result
    assert hashlib.sha256(report_file.read_bytes()).hexdigest() == completion["report_file_sha256"]
    json.dumps(result, allow_nan=False)
    with pytest.raises(FileExistsError):
        score(synthetic, tmp_path)


def test_independent_scaler_fit_and_unavailable_context_poison(synthetic):
    from src.features import FEATURE_COLUMNS, completed_daily
    from src.targets import OPEN_TARGET, target_data
    dev, _, _, receipt, bundle, _ = synthetic
    freeze = pd.Timestamp(bundle["freeze_utc"])
    eligible = completed_daily(dev, as_of=freeze - pd.Timedelta(seconds=60))
    train, _ = target_data(eligible, target=OPEN_TARGET, lag_seconds=60)
    train = train.loc[train.maturity < freeze]
    np.testing.assert_allclose(train.loc[:, FEATURE_COLUMNS].to_numpy(float).mean(axis=0), bundle["mean"])
    assert len(train) == bundle["train_rows"]
    poisoned = dev.copy()
    poisoned.loc[poisoned.index[-1], ["open", "high", "low", "close"]] *= 20
    other = freeze_model(poisoned, receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256=frame_digest(poisoned))
    for key in ("mean", "scale", "coef", "intercept", "calibration_coef", "calibration_intercept", "prior", "train_rows"):
        assert other[key] == bundle[key]


def test_future_and_target_poisoning_cannot_change_earlier_predictions(synthetic):
    _, features, _, _, bundle, sealed = synthetic
    changed = features.copy()
    changed.loc[200:, ["open", "high", "low", "close"]] *= 3
    changed["target_up"] = "DO NOT READ"
    other = predict_features(bundle, changed, expected_freeze_sha256=digest(bundle))
    assert sealed["predictions"][:200] == other["predictions"][:200]
    target_only = features.assign(target_up="POISONED")
    assert sealed == predict_features(bundle, target_only, expected_freeze_sha256=digest(bundle))


@pytest.mark.parametrize("case", ["empty", "gap", "duplicate", "overlap", "extra", "bad_price"])
def test_malformed_feature_windows_rejected(synthetic, case):
    _, features, _, _, bundle, _ = synthetic
    broken = features.copy()
    if case == "empty": broken = broken.iloc[:0]
    elif case == "gap": broken = broken.drop(index=20)
    elif case == "duplicate": broken = pd.concat([broken, broken.iloc[[0]]])
    elif case == "overlap": broken.loc[0, "timestamp"] = pd.Timestamp("2024-12-31T00:00:00Z")
    elif case == "extra": broken = broken.iloc[:-1]
    else: broken.loc[10, "open"] = -1
    with pytest.raises(ValueError):
        predict_features(bundle, broken, expected_freeze_sha256=digest(bundle))


def test_input_hashes_acknowledgement_and_overlap_binding(synthetic, tmp_path):
    _, _, outcomes, _, bundle, sealed = synthetic
    base = dict(expected_freeze_sha256=digest(bundle), expected_prediction_sha256=digest(sealed),
                expected_outcomes_sha256=frame_digest(outcomes), state_directory=tmp_path)
    with pytest.raises(ValueError, match="acknowledgement"):
        score_once(bundle, sealed, outcomes, **base)
    with pytest.raises(ValueError, match="Outcome input hash"):
        score_once(bundle, sealed, outcomes, **{**base, "expected_outcomes_sha256": "wrong"}, acknowledge_unblind=True)
    changed = outcomes.copy()
    changed.loc[10, ["open", "high", "low", "close"]] *= 2
    with pytest.raises(ValueError, match="overlap mismatch"):
        score_once(bundle, sealed, changed, **{**base, "expected_outcomes_sha256": frame_digest(changed)}, acknowledge_unblind=True)
    altered = copy.deepcopy(sealed)
    altered["predictions"][0]["logistic_calibrated"] = .999
    with pytest.raises(ValueError, match="overlap mismatch"):
        score_once(bundle, altered, outcomes, **{**base, "expected_prediction_sha256": digest(altered)}, acknowledge_unblind=True)
    assert not tmp_path.exists() or not list(tmp_path.iterdir())


def test_development_overlap_drift_and_late_labels(synthetic):
    dev, features, _, receipt, bundle, _ = synthetic
    with pytest.raises(ValueError, match="Development"):
        freeze_model(pd.concat([dev, features.iloc[:1]]), receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256=frame_digest(pd.concat([dev, features.iloc[:1]])))
    changed = copy.deepcopy(bundle)
    changed["receipt"]["source_sha256"]["ledger.py"] = "drift"
    with pytest.raises(ValueError, match="drift"):
        predict_features(changed, features, expected_freeze_sha256=digest(changed))
    changed = copy.deepcopy(bundle)
    changed["train_label_maturity_max_utc"] = "2025-01-01T00:00:00Z"
    with pytest.raises(ValueError, match="pre-holdout"):
        predict_features(changed, features, expected_freeze_sha256=digest(changed))
    changed = copy.deepcopy(bundle)
    changed["calibration_label_maturity_max_utc"] = "2025-01-01T00:00:00Z"
    with pytest.raises(ValueError, match="calibrator"):
        predict_features(changed, features, expected_freeze_sha256=digest(changed))


def test_failure_after_attempt_marker_blocks_automatic_repeat(synthetic, tmp_path, monkeypatch):
    import src.final_runner as runner
    monkeypatch.setattr(runner, "diagnostics", lambda rows: (_ for _ in ()).throw(RuntimeError("synthetic report failure")))
    with pytest.raises(RuntimeError):
        score(synthetic, tmp_path)
    assert len(list(tmp_path.glob("*.unblind.json"))) == 1
    with pytest.raises(FileExistsError):
        score(synthetic, tmp_path)


def test_outcome_window_incomplete_or_padding_rejected(synthetic, tmp_path):
    _, _, outcomes, _, bundle, sealed = synthetic
    for broken in (outcomes.iloc[:-1], outcomes.iloc[:0], pd.concat([outcomes, outcomes.iloc[-1:]])):
        with pytest.raises(ValueError):
            score_once(bundle, sealed, broken, expected_freeze_sha256=digest(bundle), expected_prediction_sha256=digest(sealed),
                       expected_outcomes_sha256="unused", state_directory=tmp_path, acknowledge_unblind=True)


def test_full_synthetic_cli_freeze_predict_score(synthetic, tmp_path, monkeypatch, capsys):
    import hashlib
    from src import research
    dev, features, outcomes, receipt, _, _ = synthetic
    from src.snapshot import save_snapshot
    snapshot = tmp_path / "synthetic-development"
    manifest = save_snapshot(snapshot, dev, start=dev.timestamp.iloc[0], end="2025-01-01T00:00:00Z", as_of="2026-01-01T00:00:00Z")
    reference_file = tmp_path / "synthetic-pin.json"
    reference_file.write_text(json.dumps(manifest))
    receipt_path = tmp_path / "receipt.json"
    receipt_path.write_text(json.dumps(receipt))
    freeze_file, prediction_file, score_file = (tmp_path / p for p in ("freeze.json", "predictions.json", "scores.json"))
    feature_file, outcome_file = tmp_path / "synthetic-features.csv", tmp_path / "synthetic-outcomes.csv"
    features.to_csv(feature_file, index=False)
    outcomes.to_csv(outcome_file, index=False)
    monkeypatch.setattr("sys.argv", ["research", "final-freeze", str(snapshot), "--receipt", str(receipt_path),
                                   "--synthetic-development-pin", "--development-pin", str(reference_file), "--expected-development-pin-sha256", digest(manifest),
                                   "--expected-development-csv-sha256", manifest["sha256"],
                                   "--expected-protocol-sha256", receipt["protocol_sha256"], "--output", str(freeze_file)])
    bundle = research.main()
    monkeypatch.setattr("sys.argv", ["research", "final-predict", "--freeze", str(freeze_file), "--features", str(feature_file),
                                   "--expected-freeze-sha256", digest(bundle), "--output", str(prediction_file)])
    sealed = research.main()
    args = ["research", "final-score", "--freeze", str(freeze_file), "--predictions", str(prediction_file), "--outcomes", str(outcome_file),
            "--expected-freeze-sha256", digest(bundle), "--expected-prediction-sha256", digest(sealed),
            "--expected-outcome-file-sha256", hashlib.sha256(outcome_file.read_bytes()).hexdigest(),
            "--state-directory", str(tmp_path / "state"), "--output", str(score_file)]
    monkeypatch.setattr("sys.argv", args)
    with pytest.raises(ValueError, match="acknowledgement"):
        research.main()
    monkeypatch.setattr("sys.argv", args + ["--acknowledge-unblind"])
    result = research.main()
    assert result["counts"]["scored"] == 361
    # Attempt record, not output filename, blocks a second score.
    monkeypatch.setattr("sys.argv", [str(tmp_path / "second-score.json") if v == str(score_file) else v for v in args] + ["--acknowledge-unblind"])
    outcome_file.unlink()
    with pytest.raises(FileExistsError):
        research.main()


def test_insufficient_and_single_class_development_rejected(synthetic):
    dev, _, _, receipt, bundle, _ = synthetic
    with pytest.raises(ValueError):
        freeze_model(dev.iloc[-100:], receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256=frame_digest(dev.iloc[-100:]))
    one_class = dev.copy()
    one_class["open"] = np.arange(len(dev), dtype=float) + 100
    one_class["close"] = one_class.open
    one_class["high"] = one_class.open * 1.01
    one_class["low"] = one_class.open * .99
    with pytest.raises(ValueError, match="two-class"):
        freeze_model(one_class, receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256=frame_digest(one_class))
    target_poisoned = dev.assign(target_up="MUST BE IGNORED")
    assert freeze_model(target_poisoned, receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256=frame_digest(dev)) == bundle


def test_scoring_never_fits_and_future_outcomes_only_change_late_losses(synthetic, tmp_path, monkeypatch):
    import src.final_runner as runner
    _, features, outcomes, _, bundle, sealed = synthetic
    monkeypatch.setattr(runner, "_model", lambda: (_ for _ in ()).throw(AssertionError("must not fit")))
    assert runner.predict_features(bundle, features, expected_freeze_sha256=digest(bundle)) == sealed
    first = score(synthetic, tmp_path / "original")
    changed = outcomes.copy()
    changed.loc[361:, ["open", "high", "low", "close"]] *= 5
    second = runner.score_once(bundle, sealed, changed, expected_freeze_sha256=digest(bundle),
                               expected_prediction_sha256=digest(sealed), expected_outcomes_sha256=frame_digest(changed),
                               state_directory=tmp_path / "synthetic-alternative-outcomes", acknowledge_unblind=True)
    assert first["uncertainty"]["daily_matched_loss"][:-3] == second["uncertainty"]["daily_matched_loss"][:-3]


def test_cli_authenticates_and_parses_same_outcome_bytes_including_last_three_rows(synthetic, tmp_path, monkeypatch, capsys):
    import hashlib
    import io
    from src import research
    _, _, outcomes, _, bundle, _ = synthetic
    payload = outcomes.to_csv(index=False).encode()
    original_reader = pd.read_csv
    parsed = original_reader(io.BytesIO(payload))
    sealed = predict_features(bundle, parsed.iloc[:361], expected_freeze_sha256=digest(bundle))
    altered = outcomes.copy()
    for i, factor in zip(altered.index[-3:], (10., .01, 10.)):
        altered.loc[i, ["open", "high", "low", "close"]] *= factor
    replacement = altered.to_csv(index=False).encode()
    freeze_file, prediction_file, outcome_file, output = (tmp_path / p for p in ("freeze.json", "predictions.json", "synthetic-outcomes.csv", "report.json"))
    freeze_file.write_text(json.dumps(bundle))
    prediction_file.write_text(json.dumps(sealed))
    outcome_file.write_bytes(payload)
    def swapping_reader(stream, *args, **kwargs):
        assert isinstance(stream, io.BytesIO), "must never reopen the authenticated pathname"
        assert stream.getvalue() == payload
        outcome_file.write_bytes(replacement)
        return original_reader(stream, *args, **kwargs)
    monkeypatch.setattr(pd, "read_csv", swapping_reader)
    monkeypatch.setattr("sys.argv", ["research", "final-score", "--freeze", str(freeze_file), "--predictions", str(prediction_file),
        "--outcomes", str(outcome_file), "--expected-freeze-sha256", digest(bundle), "--expected-prediction-sha256", digest(sealed),
        "--expected-outcome-file-sha256", hashlib.sha256(payload).hexdigest(), "--state-directory", str(tmp_path / "state"),
        "--acknowledge-unblind", "--output", str(output)])
    result = research.main()
    assert outcome_file.read_bytes() == replacement
    assert result["outcome_file_sha256"] == hashlib.sha256(payload).hexdigest()
    assert result["outcomes_sha256"] == frame_digest(parsed)
    by_time = parsed.assign(timestamp=pd.to_datetime(parsed.timestamp, utc=True)).set_index("timestamp").open
    for row, loss in zip(sealed["predictions"][-3:], result["uncertainty"]["daily_matched_loss"][-3:]):
        y = int(by_time.loc[pd.Timestamp(row["label_exit_utc"])] > by_time.loc[pd.Timestamp(row["label_entry_utc"])])
        assert loss["raw_minus_prior"] == pytest.approx((row["logistic_raw"] - y) ** 2 - (row["prior"] - y) ** 2)
    completion = json.loads(next((tmp_path / "state").glob("*.completed.json")).read_text())
    assert completion["report_path"] == str(output.resolve())
    assert completion["report_file_sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()
    assert completion["canonical_report_sha256"] == digest(json.loads(output.read_text()))


def test_report_durability_failure_never_creates_completion(synthetic, tmp_path, monkeypatch):
    import src.final_runner as runner
    real_fsync = runner.os.fsync
    calls = 0
    def fail_fsync(fd):
        nonlocal calls
        calls += 1
        if calls == 3:  # attempt file and state directory already durable
            raise OSError("synthetic durability failure")
        return real_fsync(fd)
    monkeypatch.setattr(runner.os, "fsync", fail_fsync)
    with pytest.raises(OSError, match="durability"):
        score(synthetic, tmp_path)
    assert len(list(tmp_path.glob("*.unblind.json"))) == 1
    assert not list(tmp_path.glob("*.completed.json"))
    with pytest.raises(FileExistsError):
        score(synthetic, tmp_path)


def test_wrong_independent_development_pin_rejected(synthetic):
    dev, _, _, receipt, _, _ = synthetic
    with pytest.raises(ValueError, match="pinned development frame"):
        freeze_model(dev, receipt, expected_protocol_sha256=receipt["protocol_sha256"], expected_development_frame_sha256="wrong")


def test_production_freeze_rejects_self_consistent_unapproved_reference_before_loading(synthetic, tmp_path, monkeypatch):
    from src import research
    dev, _, _, receipt, _, _ = synthetic
    pin = {"schema": 1, "source": "coinbase", "symbol": "BTC/USD", "timeframe": "1d",
           "start": "2023-01-01T00:00:00+00:00", "end_exclusive": "2025-01-01T00:00:00+00:00",
           "rows": len(dev), "availability_lag_seconds_assumed": 60, "historical_publication_verified": False,
           "sha256": "a" * 64}
    pin_file, receipt_file = tmp_path / "new-reference.json", tmp_path / "receipt.json"
    pin_file.write_text(json.dumps(pin))
    receipt_file.write_text(json.dumps(receipt))
    monkeypatch.setattr(research, "load_snapshot", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must reject before reading substitute data")))
    monkeypatch.setattr("sys.argv", ["research", "final-freeze", "unapproved-directory", "--receipt", str(receipt_file),
        "--expected-protocol-sha256", receipt["protocol_sha256"], "--development-pin", str(pin_file),
        "--expected-development-pin-sha256", digest(pin), "--expected-development-csv-sha256", pin["sha256"],
        "--output", str(tmp_path / "freeze.json")])
    with pytest.raises(ValueError, match="Unapproved production"):
        research.main()


def test_report_write_failure_leaves_attempt_and_no_completion(synthetic, tmp_path, monkeypatch):
    original_open = Path.open
    def fail_report(path, *args, **kwargs):
        if path.name == "final-report.json":
            raise OSError("synthetic report write failure")
        return original_open(path, *args, **kwargs)
    monkeypatch.setattr(Path, "open", fail_report)
    with pytest.raises(OSError, match="write failure"):
        score(synthetic, tmp_path)
    assert list(tmp_path.glob("*.unblind.json"))
    assert not list(tmp_path.glob("*.completed.json"))

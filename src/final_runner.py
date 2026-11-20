"""Hash-bound frozen final runner. Real holdout execution remains unauthorized."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.special import expit

from .baseline import _model
from .features import FEATURE_COLUMNS, feature_frame, prepare_daily
from .ledger import EPS, METHODS, calibrate, diagnostics, forecast_ledger
from .execution import _account
from .protocol import audit_holdout_timing, protocol_receipt
from .snapshot import DAY, utc
from .features import completed_daily
from .targets import OPEN_TARGET, target_data
from .uncertainty import paired_uncertainty
from .runtime import require_reference_runtime


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def frame_digest(frame):
    data = prepare_daily(frame)
    for name in ("open", "high", "low", "close", "volume"):
        data[name] = data[name].astype(float)
    return hashlib.sha256(data.to_csv(index=False).encode()).hexdigest()


def _check_receipt(receipt):
    require_reference_runtime()
    if protocol_receipt(receipt["protocol"]) != receipt:
        raise ValueError("Source, runtime or protocol drift")


def _bounds(receipt):
    p = receipt["protocol"]
    start, end = utc(p["holdout_start"]), utc(p["holdout_end_exclusive"])
    lag = pd.Timedelta(seconds=p["lag_seconds_assumed"])
    times = pd.date_range(start, end, freq="D", inclusive="left")
    # Only features whose full exit candle is known by the fixed end are scored.
    entry_days = 2 + int(lag // DAY)
    features = times[times + (entry_days + 2) * DAY + lag <= end]
    if features.empty:
        raise ValueError("Empty final window")
    outcome_end = features[-1] + (entry_days + 1) * DAY
    return start, end, lag, features, outcome_end


def _exact(frame, first, last):
    data = prepare_daily(frame)
    expected = pd.Series(pd.date_range(first, last, freq="D"), name="timestamp")
    if not data.timestamp.equals(expected):
        raise ValueError("Incomplete, overlapping or out-of-domain daily input")
    return data


def freeze_model(development, receipt, *, expected_protocol_sha256):
    """Fit only strictly pre-boundary matured labels. Never accepts holdout rows."""
    _check_receipt(receipt)
    if receipt["protocol_sha256"] != expected_protocol_sha256:
        raise ValueError("Protocol hash mismatch")
    p = receipt["protocol"]
    start, _, lag, _, _ = _bounds(receipt)
    dev = prepare_daily(development)
    if (dev.timestamp >= start).any() or dev.timestamp.iloc[-1] != start - DAY:
        raise ValueError("Development must end at the last pre-holdout candle, with no overlap")
    freeze = start - pd.Timedelta(nanoseconds=1)
    available = completed_daily(dev, as_of=freeze - lag)
    train, _ = target_data(available, target=OPEN_TARGET, lag_seconds=p["lag_seconds_assumed"])
    train = train.loc[train.maturity < freeze]
    if len(train) < p["min_train"] or train.target_up.nunique() != 2:
        raise ValueError("Insufficient matured two-class final training set")
    history = forecast_ledger(dev, holdout_start=start, as_of=freeze,
                              lag_seconds=p["lag_seconds_assumed"], min_train=p["min_train"],
                              test_size=p["test_size"], min_calibration=p["min_calibration"], target=OPEN_TARGET)
    cal, status, eligible = calibrate(history["ledger"], freeze=freeze, min_rows=p["min_calibration"])
    if cal is None or status != "fitted":
        raise ValueError("No matured earlier-OOF calibrator; final primary cannot use fallback")
    model = _model().fit(train.loc[:, FEATURE_COLUMNS].to_numpy(float), train.target_up.to_numpy(int))
    scaler, logistic = model.steps[0][1], model.steps[1][1]
    context = dev.iloc[-14:].copy()
    if len(context) != 14:
        raise ValueError("Missing trailing feature context")
    context["timestamp"] = context.timestamp.map(lambda t: t.isoformat())
    return {"schema": 1, "receipt": receipt, "freeze_utc": freeze.isoformat(),
            "development_sha256": frame_digest(dev), "feature_columns": list(FEATURE_COLUMNS),
            "mean": scaler.mean_.tolist(), "scale": scaler.scale_.tolist(),
            "coef": logistic.coef_[0].tolist(), "intercept": float(logistic.intercept_[0]),
            "calibration_coef": float(cal.coef_[0, 0]), "calibration_intercept": float(cal.intercept_[0]),
            "prior": float(train.target_up.mean()), "train_rows": len(train),
            "train_feature_start_utc": train.timestamp.iloc[0].isoformat(),
            "train_feature_end_utc": train.timestamp.iloc[-1].isoformat(),
            "train_label_maturity_max_utc": train.maturity.iloc[-1].isoformat(),
            "calibration_rows": len(eligible),
            "calibration_label_maturity_max_utc": max(r["label_maturity_utc"] for r in eligible),
            "feature_context": context.to_dict("records"),
            "context_policy": "context only, never final-fit input unless independently available/matured; latest pre-boundary candle becomes available after freeze"}


def predict_features(bundle, features, *, expected_freeze_sha256):
    """No label construction or outcomes; trailing features only, fixed coefficients."""
    if digest(bundle) != expected_freeze_sha256:
        raise ValueError("Frozen model hash mismatch")
    receipt = bundle["receipt"]
    _check_receipt(receipt)
    start, end, lag, dates, _ = _bounds(receipt)
    data = _exact(features, dates[0], dates[-1])
    context = _exact(pd.DataFrame(bundle["feature_context"]), start - 14 * DAY, start - DAY)
    merged = pd.concat([context, data], ignore_index=True)
    feature_values = feature_frame(merged).iloc[14:].reset_index(drop=True)
    x = feature_values.loc[:, FEATURE_COLUMNS].to_numpy(float)
    if not np.isfinite(x).all():
        raise ValueError("Incomplete finite feature support")
    raw = expit(((x - np.array(bundle["mean"])) / np.array(bundle["scale"])) @ np.array(bundle["coef"]) + bundle["intercept"])
    clipped = np.clip(raw, EPS, 1 - EPS)
    calibrated = expit(bundle["calibration_coef"] * np.log(clipped / (1 - clipped)) + bundle["calibration_intercept"])
    entry_days = 2 + int(lag // DAY)
    rows = []
    for i, stamp in enumerate(dates):
        rows.append({"feature_open_utc": stamp.isoformat(), "decision_utc": (stamp + DAY + lag).isoformat(),
                     "candle_available_utc": (stamp + DAY + lag).isoformat(),
                     "label_entry_utc": (stamp + entry_days * DAY).isoformat(),
                     "label_exit_utc": (stamp + (entry_days + 1) * DAY).isoformat(),
                     "label_maturity_utc": (stamp + (entry_days + 2) * DAY + lag).isoformat(),
                     "target_name": OPEN_TARGET, "fold": 1, "calibrated": True,
                     "freeze_utc": bundle["freeze_utc"],
                     "train_label_maturity_max_utc": bundle["train_label_maturity_max_utc"],
                     "calibration_label_maturity_max_utc": bundle["calibration_label_maturity_max_utc"],
                     "prior": bundle["prior"], "neutral": .5,
                     "persistence": float(np.clip(int(feature_values.loc[i, "return_1d"] > 0), EPS, 1 - EPS)),
                     "logistic_raw": float(raw[i]), "logistic_calibrated": float(calibrated[i])})
    audit_holdout_timing(rows, receipt, expected_protocol_sha256=receipt["protocol_sha256"])
    return {"schema": 1, "evaluation_scope": "sealed_features_no_outcomes", "freeze_sha256": expected_freeze_sha256,
            "features_sha256": frame_digest(data), "first_feature": dates[0].isoformat(),
            "last_feature": dates[-1].isoformat(), "rows": len(rows), "predictions": rows}


def check_score_seals(bundle, sealed, *, expected_freeze_sha256, expected_prediction_sha256, state_directory):
    """Reject known prior attempts and drift before CLI reads an outcome file."""
    if digest(bundle) != expected_freeze_sha256 or digest(sealed) != expected_prediction_sha256:
        raise ValueError("Freeze or prediction hash mismatch")
    _check_receipt(bundle["receipt"])
    marker = Path(state_directory) / (expected_freeze_sha256 + ".unblind.json")
    if marker.exists():
        raise FileExistsError(marker)


def score_once(bundle, sealed, outcomes, *, expected_freeze_sha256, expected_prediction_sha256,
               expected_outcomes_sha256, state_directory, acknowledge_unblind=False):
    """Exclusive local attempt marker; deleting it/using another directory can bypass it.

    Failures after marker creation stay marked. No automated retry/reset and no
    claim of globally enforcing a single evaluation.
    """
    if not acknowledge_unblind:
        raise ValueError("Explicit unblind acknowledgement required; no evaluation authorized in this milestone")
    check_score_seals(bundle, sealed, expected_freeze_sha256=expected_freeze_sha256, expected_prediction_sha256=expected_prediction_sha256, state_directory=state_directory)
    start, end, _, dates, last_outcome = _bounds(bundle["receipt"])
    data = _exact(outcomes, start, last_outcome)
    if frame_digest(data) != expected_outcomes_sha256:
        raise ValueError("Outcome input hash mismatch")
    # Independently rebuild feature-only predictions to bind outcome overlap to
    # sealed inputs. Never fit or calibrate in prediction/scoring stages.
    rebuilt = predict_features(bundle, data.loc[data.timestamp <= dates[-1]], expected_freeze_sha256=expected_freeze_sha256)
    if rebuilt != sealed:
        raise ValueError("Sealed predictions or feature/outcome overlap mismatch")
    state = Path(state_directory)
    state.mkdir(parents=True, exist_ok=True)
    marker = state / (expected_freeze_sha256 + ".unblind.json")
    with marker.open("x", encoding="utf-8") as stream:
        json.dump({"status": "attempt_started", "freeze_sha256": expected_freeze_sha256,
                   "prediction_sha256": expected_prediction_sha256, "outcomes_sha256": expected_outcomes_sha256}, stream)
    # Only after reserving the local attempt do we construct labels/metrics.
    by_time = data.set_index("timestamp").open
    rows = []
    for row in sealed["predictions"]:
        entry, exit_price = (float(by_time.loc[utc(row[k])]) for k in ("label_entry_utc", "label_exit_utc"))
        rows.append({**row, "target_up": int(exit_price > entry)})
    protocol = bundle["receipt"]["protocol"]
    report = {"evaluation_scope": "frozen_final", "protocol": {"target_name": OPEN_TARGET,
              "holdout_start": start.isoformat(), "as_of": end.isoformat()}, "ledger": rows}
    uncertainty = paired_uncertainty(report, block_lengths=tuple(protocol["block_lengths_days"]),
                                     replicates=protocol["bootstrap_replicates"], seed=protocol["bootstrap_seed"],
                                     gap_policy=protocol["gap_policy"], final_domain=(start, end))
    primary = uncertainty["comparisons"]["calibrated_minus_prior"]
    cost_start = utc(rows[0]["label_entry_utc"])
    cost_bars = data.loc[data.timestamp >= cost_start].reset_index(drop=True)
    cost_scenarios = []
    for fee_bps, slip_bps in protocol["cost_scenarios_fee_slippage_bps"]:
        arguments = dict(capital=1000., fee=fee_bps / 10000, slip=slip_bps / 10000)
        strategies = {method: _account(cost_bars, {utc(r["label_entry_utc"]): int(r[method] > protocol["threshold"]) for r in rows}, **arguments) for method in METHODS}
        strategies["no_trade"] = _account(cost_bars, {}, **arguments)
        strategies["buy_and_hold"] = _account(cost_bars, {cost_start: 1}, **arguments)
        cost_scenarios.append({"fee_bps": fee_bps, "slippage_bps": slip_bps, "threshold": protocol["threshold"], "strategies": strategies})
    success = primary["mean_daily_brier_difference"] < 0 and all(
        item["status"] == "computed" and item["ci95"][1] < 0 for item in primary["block_sensitivity"])
    result = {"scope": "frozen_final", "freeze_sha256": expected_freeze_sha256,
            "prediction_sha256": expected_prediction_sha256, "outcomes_sha256": expected_outcomes_sha256,
            "protocol_sha256": bundle["receipt"]["protocol_sha256"], "marker": str(marker),
            "counts": {"predicted": len(rows), "scored": len(rows), "prior_support": len(rows), "calibration_fallback": 0},
            "domains": {"first_feature": dates[0].isoformat(), "last_feature": dates[-1].isoformat(),
                        "first_outcome_candle": start.isoformat(), "last_outcome_candle": last_outcome.isoformat(),
                        "holdout_end_exclusive": end.isoformat()},
            "fit": {k: bundle[k] for k in ("freeze_utc", "train_rows", "train_feature_start_utc", "train_feature_end_utc", "train_label_maturity_max_utc", "calibration_rows", "calibration_label_maturity_max_utc")},
            "receipt": bundle["receipt"],
            "scores_identical_rows": diagnostics(rows), "uncertainty": uncertainty,
            "execution_diagnostics": {"scope": "frozen_final_descriptive_only", "initial_quote": 1000.,
                                      "first_open": cost_start.isoformat(), "terminal_open": last_outcome.isoformat(),
                                      "window_opens": len(cost_bars), "scenarios": cost_scenarios,
                                      "caveat": "bucket first-trade proxy, not executable fill evidence; costs do not enter statistical success criterion"},
            "predeclared_statistical_success": bool(success),
            "interpretation": "No retuning; statistical criterion is not profitability or executable edge"}
    with (state / (expected_freeze_sha256 + ".completed.json")).open("x", encoding="utf-8") as stream:
        json.dump({"status": "metrics_completed", "core_report_sha256": digest(result),
                   "scored_rows": len(rows)}, stream)
    return result

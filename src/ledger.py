"""Development-only, frozen-fold OOF forecasts with strictly past OOF calibration."""
from __future__ import annotations

import hashlib
from importlib.metadata import version
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from .baseline import _model, _scores
from .features import FEATURE_COLUMNS
from .snapshot import DAY, development_only, utc
from .targets import CLOSE_TARGET, OPEN_TARGET, target_data

METHODS = ("prior", "persistence", "neutral", "logistic_raw", "logistic_calibrated")
EPS = 1e-6


def _logit(values):
    p = np.clip(np.asarray(values, dtype=float), EPS, 1 - EPS)
    return np.log(p / (1 - p)).reshape(-1, 1)


def calibrate(history, *, freeze, min_rows):
    """Use distinct earlier-fold raw OOF scores; strict maturity excludes ties."""
    eligible = [r for r in history if utc(r["label_maturity_utc"]) < freeze
                and r["raw_model_status"] == "fitted"]
    if len({r["feature_open_utc"] for r in eligible}) != len(eligible):
        raise ValueError("Duplicate calibration OOF observation")
    status = "fitted"
    if len(eligible) < min_rows:
        status = "warmup_insufficient_oof"
    elif len({r["target_up"] for r in eligible}) < 2:
        status = "warmup_single_class"
    model = None
    if status == "fitted":
        model = LogisticRegression(C=1.0, max_iter=2000, random_state=0)
        model.fit(_logit([r["logistic_raw"] for r in eligible]), [r["target_up"] for r in eligible])
    return model, status, eligible


def diagnostics(rows, methods=METHODS):
    """Every method receives exactly the same observations, with fixed bins."""
    result = {}
    y = np.array([r["target_up"] for r in rows], dtype=int)
    for method in methods:
        p = np.array([r[method] for r in rows], dtype=float)
        bins = []
        for i in range(10):
            mask = (p >= i / 10) & ((p < (i + 1) / 10) if i < 9 else (p <= 1))
            if mask.any():
                bins.append({"lower": i / 10, "upper": (i + 1) / 10, "rows": int(mask.sum()),
                             "mean_probability": float(p[mask].mean()), "up_fraction": float(y[mask].mean())})
        result[method] = {"rows": len(rows), "scores": _scores(y, p) if len(rows) else None,
                          "reliability_bins": bins,
                          "ece_10": sum(b["rows"] * abs(b["mean_probability"] - b["up_fraction"]) for b in bins) / len(rows) if rows else None}
    return result


def forecast_ledger(frame, *, holdout_start, as_of, lag_seconds=60,
                    min_train=80, test_size=30, min_calibration=30,
                    snapshot_sha256=None, source_kind="synthetic", target=CLOSE_TARGET):
    """Fixed anchored blocks: future length cannot move earlier fold boundaries.

    Model and calibrator freeze at the first decision of each block. No
    within-block refitting. Forecast outcomes are attached retrospectively.
    """
    if min_train < 20 or test_size < 1 or min_calibration < 2:
        raise ValueError("Invalid training, block or calibration size")
    cutoff = utc(as_of)
    dev = development_only(frame, holdout_start=holdout_start, as_of=cutoff, lag_seconds=lag_seconds)
    data, purge_prefix = target_data(dev, target=target, lag_seconds=lag_seconds)
    lag = pd.Timedelta(seconds=lag_seconds)
    data["decision"] = data.timestamp + DAY + lag
    data = data.loc[data.maturity <= cutoff].reset_index(drop=True)
    if len(data) <= min_train + purge_prefix:
        raise ValueError("Insufficient development data for an OOF block")
    rows, folds = [], []
    x = data.loc[:, FEATURE_COLUMNS].to_numpy(dtype=float)
    y = data.target_up.to_numpy(dtype=int)
    for fold, begin in enumerate(range(min_train + purge_prefix, len(data), test_size), 1):
        end = min(begin + test_size, len(data))
        freeze = data.loc[begin, "decision"]
        train = np.flatnonzero((data.index < begin) & (data.maturity < freeze))
        if len(train) < min_train:
            raise ValueError("Not enough strictly matured training labels")
        prior = float(y[train].mean())
        raw_model = None
        raw_status = "single_class_prior_fallback"
        if len(np.unique(y[train])) == 2:
            raw_model = _model()
            raw_model.fit(x[train], y[train])
            raw_status = "fitted"
        calibrator, cal_status, eligible = calibrate(rows, freeze=freeze, min_rows=min_calibration)
        metadata = {"fold": fold, "freeze_utc": freeze.isoformat(),
                    "train_start_utc": data.loc[train[0], "timestamp"].isoformat(),
                    "train_end_utc": data.loc[train[-1], "timestamp"].isoformat(),
                    "train_label_maturity_max_utc": data.loc[train[-1], "maturity"].isoformat(),
                    "train_rows": len(train), "purged_rows": begin - len(train),
                    "test_start_utc": data.loc[begin, "timestamp"].isoformat(),
                    "test_end_utc": data.loc[end - 1, "timestamp"].isoformat(),
                    "calibration_rows": len(eligible), "calibration_status": cal_status,
                    "calibration_label_maturity_max_utc": max((r["label_maturity_utc"] for r in eligible), default=None),
                    "calibration_feature_start_utc": min((r["feature_open_utc"] for r in eligible), default=None),
                    "calibration_feature_end_utc": max((r["feature_open_utc"] for r in eligible), default=None),
                    "calibration_coefficient": float(calibrator.coef_[0, 0]) if calibrator is not None else None,
                    "calibration_intercept": float(calibrator.intercept_[0]) if calibrator is not None else None,
                    "raw_model_status": raw_status}
        folds.append(metadata)
        predictions = raw_model.predict_proba(x[begin:end])[:, 1] if raw_model is not None else np.full(end - begin, prior)
        calibrated = calibrator.predict_proba(_logit(predictions))[:, 1] if calibrator is not None and raw_model is not None else predictions
        # Append only after calibration was frozen: same-fold outcomes never enter it.
        block = []
        for offset, i in enumerate(range(begin, end)):
            block.append({**metadata, "feature_open_utc": data.loc[i, "timestamp"].isoformat(),
                          "candle_available_utc": data.loc[i, "decision"].isoformat(),
                          "decision_utc": data.loc[i, "decision"].isoformat(),
                          "target_name": target,
                          "label_entry_utc": data.loc[i, "label_entry"].isoformat(),
                          "label_exit_utc": data.loc[i, "label_exit"].isoformat(),
                          **({"label_close_utc": data.loc[i, "label_exit"].isoformat()} if target == CLOSE_TARGET else {}),
                          "label_maturity_utc": data.loc[i, "maturity"].isoformat(),
                          **({"label_entry_reference_open": float(data.loc[i, "label_entry_reference_open"]),
                              "label_exit_reference_open": float(data.loc[i, "label_exit_reference_open"])} if target == OPEN_TARGET else {}),
                          "target_up": int(y[i]), "prior": prior, "neutral": .5,
                          "persistence": float(np.clip(int(data.loc[i, "return_1d"] > 0), EPS, 1 - EPS)),
                          "logistic_raw": float(predictions[offset]),
                          "logistic_calibrated": float(calibrated[offset]),
                          "calibrated": calibrator is not None and raw_model is not None})
        rows.extend(block)
    paired = [r for r in rows if r["calibrated"]]
    code_hash = hashlib.sha256(b"".join(Path(__file__).with_name(p).read_bytes() for p in ("ledger.py", "baseline.py", "features.py", "snapshot.py", "research.py", "targets.py"))).hexdigest()
    canonical = dev.copy()
    numeric = ["open", "high", "low", "close", "volume"]
    canonical[numeric] = canonical[numeric].astype(float)
    return {"schema": 1, "research_only": True, "source_kind": source_kind,
            "snapshot_sha256": snapshot_sha256,
            "development_sha256": hashlib.sha256(canonical.to_csv(index=False).encode()).hexdigest(),
            "code_sha256": code_hash,
            "software_versions": {name: version(name) for name in ("numpy", "pandas", "scikit-learn")},
            "protocol": {"target_name": target,
                         "label_maturity_rule": "exit candle completed + lag" if target == OPEN_TARGET else "target close + lag",
                         "holdout_start": str(holdout_start), "as_of": cutoff.isoformat(),
                         "lag_seconds_assumed": lag_seconds, "min_train": min_train,
                         "test_size": test_size, "min_calibration": min_calibration,
                         "calibration": "L2 logistic on logit(raw past OOF), C=1; strictly matured before fold freeze",
                         "persistence": "previous return sign, probabilities clipped to [1e-6,1-1e-6]"},
            "counts": {"oof": len(rows), "paired_calibrated": len(paired),
                       "non_calibrated_fallback": len(rows) - len(paired),
                       "calibration_status_rows": {status: sum(r["calibration_status"] == status for r in rows)
                                                   for status in sorted({r["calibration_status"] for r in rows})}},
            "all_oof": diagnostics(rows), "paired_calibrated": diagnostics(paired),
            "folds": folds, "ledger": rows}

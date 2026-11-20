"""Immutable pre-holdout receipt and metadata-only timing preflight, no scoring."""
import hashlib
from importlib.metadata import version
import json
import sys
from pathlib import Path

import pandas as pd

from .snapshot import DAY, midnight, utc

SOURCES = ("baseline.py", "features.py", "snapshot.py", "targets.py", "ledger.py", "execution.py", "uncertainty.py", "protocol.py", "research.py")


def protocol_receipt(protocol):
    """Content-address protocol and exact scripts; does not authorize data access."""
    if protocol.get("state") != "locked_preholdout_no_evaluation_authorized":
        raise ValueError("Protocol must remain locked; no evaluation authorized")
    if protocol["target_name"] != "execution_open_direction" or midnight(protocol["holdout_start"]) >= midnight(protocol["holdout_end_exclusive"]):
        raise ValueError("Invalid frozen target or dates")
    return {"protocol": protocol,
            "protocol_sha256": hashlib.sha256(json.dumps(protocol, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "source_sha256": {p: hashlib.sha256(Path(__file__).with_name(p).read_bytes()).hexdigest() for p in SOURCES},
            "versions": {p: version(p) for p in ("numpy", "pandas", "scikit-learn")},
            "python": sys.version,
            "status": "receipt only; dependency lock and approval still required before final evaluation"}


def audit_holdout_timing(rows, receipt, *, expected_protocol_sha256):
    """Validate metadata only. Never read outcomes or probabilities or score.

    Synthetic callers demonstrate gates. No real holdout file has been opened.
    """
    current = protocol_receipt(receipt["protocol"])
    if receipt != current or receipt["protocol_sha256"] != expected_protocol_sha256:
        raise ValueError("Protocol, source or environment receipt drift")
    protocol = receipt["protocol"]
    start, end = midnight(protocol["holdout_start"]), midnight(protocol["holdout_end_exclusive"])
    lag = pd.Timedelta(seconds=protocol["lag_seconds_assumed"])
    seen, prior_feature, fit_bounds = set(), None, None
    if not rows:
        raise ValueError("Empty timing metadata")
    for row in rows:
        feature = midnight(row["feature_open_utc"])
        decision, available = utc(row["decision_utc"]), utc(row["candle_available_utc"])
        entry, exit_time, maturity = (utc(row[k]) for k in ("label_entry_utc", "label_exit_utc", "label_maturity_utc"))
        freeze = utc(row["freeze_utc"])
        if feature in seen or not start <= feature < end or not start <= decision < end:
            raise ValueError("Duplicate or out-of-period holdout decision")
        if prior_feature is not None and feature - prior_feature != DAY:
            raise ValueError("Gapped or unordered holdout metadata")
        prior_feature = feature
        bounds = (row["freeze_utc"], row["train_label_maturity_max_utc"], row["calibration_label_maturity_max_utc"])
        if fit_bounds is not None and bounds != fit_bounds:
            raise ValueError("Final fit bounds changed during holdout")
        fit_bounds = bounds
        seen.add(feature)
        if row["target_name"] != protocol["target_name"] or available != feature + DAY + lag or decision < available:
            raise ValueError("Target or availability drift")
        if entry != decision.floor("D") + DAY or exit_time != entry + DAY or maturity != exit_time + DAY + lag:
            raise ValueError("Execution target timing drift")
        if exit_time >= end or maturity > end:
            raise ValueError("Outcome outside frozen evaluation window")
        if not freeze < start or not utc(row["train_label_maturity_max_utc"]) < freeze:
            raise ValueError("Final model not frozen on pre-holdout labels")
        if row["calibration_label_maturity_max_utc"] is not None and not utc(row["calibration_label_maturity_max_utc"]) < freeze:
            raise ValueError("Final calibrator includes late labels")
    return {"status": "timing metadata checked; no holdout scores computed", "rows": len(rows),
            "protocol_sha256": expected_protocol_sha256}

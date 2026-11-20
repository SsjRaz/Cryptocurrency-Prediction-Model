"""Synthetic holdout timestamps only; never real holdout prices or outcomes."""
import copy
import json
from pathlib import Path

import pytest

from src.protocol import audit_holdout_timing, protocol_receipt


def receipt():
    return protocol_receipt(json.loads(Path("protocols/preholdout.json").read_text()))


def metadata():
    return [{"feature_open_utc": "2025-01-01T00:00:00Z", "decision_utc": "2025-01-02T00:01:00Z",
             "candle_available_utc": "2025-01-02T00:01:00Z", "label_entry_utc": "2025-01-03T00:00:00Z",
             "label_exit_utc": "2025-01-04T00:00:00Z", "label_maturity_utc": "2025-01-05T00:01:00Z",
             "freeze_utc": "2024-12-31T23:59:00Z", "train_label_maturity_max_utc": "2024-12-31T00:01:00Z",
             "calibration_label_maturity_max_utc": "2024-12-30T00:01:00Z", "target_name": "execution_open_direction"}]


def test_receipt_and_metadata_only_invariance():
    frozen = receipt()
    rows = metadata()
    original = audit_holdout_timing(rows, frozen, expected_protocol_sha256=frozen["protocol_sha256"])
    rows[0]["target_up"] = "POISONED, MUST NEVER READ"
    rows[0]["logistic_calibrated"] = float("nan")
    assert original == audit_holdout_timing(rows, frozen, expected_protocol_sha256=frozen["protocol_sha256"])
    assert "no holdout scores" in original["status"]


@pytest.mark.parametrize("field,value", [("freeze_utc", "2025-01-01T00:00:00Z"),
    ("train_label_maturity_max_utc", "2024-12-31T23:59:00Z"),
    ("calibration_label_maturity_max_utc", "2025-01-02T00:00:00Z"),
    ("label_entry_utc", "2025-01-02T00:00:00Z"), ("feature_open_utc", "2024-12-31T00:00:00Z"),
    ("decision_utc", "2025-01-02T00:00:00Z"), ("target_name", "close_direction")])
def test_strict_preflight_rejects_late_labels_and_drift(field, value):
    frozen = receipt()
    rows = metadata()
    rows[0][field] = value
    with pytest.raises(ValueError):
        audit_holdout_timing(rows, frozen, expected_protocol_sha256=frozen["protocol_sha256"])


def test_modified_protocol_receipt_or_code_rejected():
    frozen = receipt()
    modified = copy.deepcopy(frozen)
    modified["protocol"]["threshold"] = .6
    with pytest.raises(ValueError, match="drift"):
        audit_holdout_timing(metadata(), modified, expected_protocol_sha256=frozen["protocol_sha256"])
    modified = copy.deepcopy(frozen)
    modified["source_sha256"]["ledger.py"] = "different"
    with pytest.raises(ValueError, match="drift"):
        audit_holdout_timing(metadata(), modified, expected_protocol_sha256=frozen["protocol_sha256"])
    with pytest.raises(ValueError, match="drift"):
        audit_holdout_timing(metadata(), frozen, expected_protocol_sha256="wrong")


def test_gaps_changed_fit_and_empty_metadata_rejected():
    from datetime import timedelta
    import pandas as pd
    frozen = receipt()
    with pytest.raises(ValueError, match="Empty"):
        audit_holdout_timing([], frozen, expected_protocol_sha256=frozen["protocol_sha256"])
    rows = metadata()
    later = copy.deepcopy(rows[0])
    for field in ("feature_open_utc", "decision_utc", "candle_available_utc", "label_entry_utc", "label_exit_utc", "label_maturity_utc"):
        later[field] = (pd.Timestamp(later[field]) + timedelta(days=2)).isoformat()
    with pytest.raises(ValueError, match="Gapped"):
        audit_holdout_timing(rows + [later], frozen, expected_protocol_sha256=frozen["protocol_sha256"])
    for field in ("feature_open_utc", "decision_utc", "candle_available_utc", "label_entry_utc", "label_exit_utc", "label_maturity_utc"):
        later[field] = (pd.Timestamp(later[field]) - timedelta(days=1)).isoformat()
    later["freeze_utc"] = "2024-12-31T23:58:00Z"
    with pytest.raises(ValueError, match="bounds changed"):
        audit_holdout_timing(rows + [later], frozen, expected_protocol_sha256=frozen["protocol_sha256"])


def test_protocol_receipt_cli_and_uncertainty_cli(tmp_path, monkeypatch, capsys):
    from src import research
    receipt_file = tmp_path / "receipt.json"
    monkeypatch.setattr("sys.argv", ["research", "protocol-receipt", "protocols/preholdout.json", "--output", str(receipt_file)])
    result = research.main()
    assert json.loads(receipt_file.read_text()) == result
    with pytest.raises(FileExistsError):
        research.main()
    # Deliberately synthetic development input; never a holdout file.
    synthetic = {"protocol": {"holdout_start": "2025-01-01T00:00:00Z", "as_of": "2024-12-31T00:00:00Z", "target_name": "execution_open_direction"}, "ledger": []}
    input_file, output_file = tmp_path / "synthetic-development.json", tmp_path / "uncertainty.json"
    input_file.write_text(json.dumps(synthetic))
    monkeypatch.setattr("sys.argv", ["research", "uncertainty", "--ledger", str(input_file), "--output", str(output_file)])
    result = research.main()
    assert result["population"]["paired"] == 0
    assert json.loads(output_file.read_text()) == result

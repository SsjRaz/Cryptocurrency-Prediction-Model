"""Snapshot acquisition and development-only research entry point."""
import argparse
import hashlib
import io
import json
from pathlib import Path

from .baseline import walk_forward
from .ledger import forecast_ledger
from .execution import DEFAULT_SCENARIOS, simulate
from .targets import CLOSE_TARGET, OPEN_TARGET
from .uncertainty import paired_uncertainty
from .protocol import protocol_receipt, audit_holdout_timing
from .final_runner import digest, frame_digest, freeze_model, predict_features, score_once, check_score_seals, check_freeze_receipt
from .snapshot import acquire, development_only, load_snapshot, save_snapshot, midnight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    fetch = sub.add_parser("acquire")
    fetch.add_argument("directory", type=Path)
    fetch.add_argument("--start", required=True)
    fetch.add_argument("--end", required=True)
    fetch.add_argument("--as-of", required=True)
    replay = sub.add_parser("development")
    replay.add_argument("directory", type=Path)
    replay.add_argument("--holdout-start", required=True)
    replay.add_argument("--as-of", required=True)
    replay.add_argument("--splits", type=int, default=5)
    ledger = sub.add_parser("ledger")
    ledger.add_argument("directory", type=Path)
    ledger.add_argument("--target", choices=(CLOSE_TARGET, OPEN_TARGET), default=CLOSE_TARGET)
    ledger.add_argument("--holdout-start", required=True)
    ledger.add_argument("--as-of", required=True)
    ledger.add_argument("--min-train", type=int, default=80)
    ledger.add_argument("--test-size", type=int, default=30)
    ledger.add_argument("--min-calibration", type=int, default=30)
    ledger.add_argument("--output", type=Path, required=True)
    execute = sub.add_parser("simulate")
    execute.add_argument("directory", type=Path)
    execute.add_argument("--ledger", type=Path, required=True)
    execute.add_argument("--as-of", required=True)
    execute.add_argument("--window-start", required=True)
    execute.add_argument("--terminal-open", required=True)
    execute.add_argument("--initial-quote", type=float, default=1000.)
    execute.add_argument("--scenarios", type=Path, help="Predetermined JSON list of named cost/threshold scenarios")
    execute.add_argument("--output", type=Path, required=True)
    uncertainty = sub.add_parser("uncertainty")
    uncertainty.add_argument("--ledger", type=Path, required=True)
    uncertainty.add_argument("--output", type=Path, required=True)
    uncertainty.add_argument("--gap-policy", choices=("reject", "segments"), default="reject")
    receipt = sub.add_parser("protocol-receipt")
    receipt.add_argument("protocol", type=Path)
    receipt.add_argument("--output", type=Path, required=True)
    audit = sub.add_parser("holdout-timing-preflight", help="Metadata-only validator; no scoring or forecast generation")
    audit.add_argument("--metadata", type=Path, required=True)
    audit.add_argument("--receipt", type=Path, required=True)
    audit.add_argument("--expected-protocol-sha256", required=True)
    audit.add_argument("--output", type=Path, required=True)
    final_freeze = sub.add_parser("final-freeze", help="Pre-holdout data only; no future fitting")
    final_freeze.add_argument("directory", type=Path)
    final_freeze.add_argument("--receipt", type=Path, required=True)
    final_freeze.add_argument("--expected-protocol-sha256", required=True)
    final_freeze.add_argument("--expected-development-csv-sha256", required=True)
    final_freeze.add_argument("--development-pin", type=Path, required=True)
    final_freeze.add_argument("--synthetic-development-pin", action="store_true", help="Explicit non-production fixture pin; never infer approval from a new manifest")
    final_freeze.add_argument("--expected-development-pin-sha256", required=True)
    final_freeze.add_argument("--output", type=Path, required=True)
    final_predict = sub.add_parser("final-predict", help="Feature-only input; no labels or scores")
    final_predict.add_argument("--freeze", type=Path, required=True)
    final_predict.add_argument("--features", type=Path, required=True)
    final_predict.add_argument("--expected-freeze-sha256", required=True)
    final_predict.add_argument("--output", type=Path, required=True)
    final_score = sub.add_parser("final-score", help="Unblind only after separately authorized; local attempt marker")
    final_score.add_argument("--freeze", type=Path, required=True)
    final_score.add_argument("--predictions", type=Path, required=True)
    final_score.add_argument("--outcomes", type=Path, required=True)
    final_score.add_argument("--expected-freeze-sha256", required=True)
    final_score.add_argument("--expected-prediction-sha256", required=True)
    final_score.add_argument("--expected-outcome-file-sha256", required=True)
    final_score.add_argument("--state-directory", type=Path, required=True)
    final_score.add_argument("--acknowledge-unblind", action="store_true")
    final_score.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command in ("final-freeze", "final-predict", "final-score"):
        if args.output.exists():
            raise FileExistsError(args.output)
        if args.command == "final-freeze":
            reference = json.loads(args.development_pin.read_bytes())
            if digest(reference) != args.expected_development_pin_sha256 or reference["sha256"] != args.expected_development_csv_sha256:
                raise ValueError("Independent development pin hash mismatch")
            required = {"schema", "source", "symbol", "timeframe", "start", "end_exclusive", "rows", "availability_lag_seconds_assumed", "historical_publication_verified", "sha256"}
            if not required.issubset(reference):
                raise ValueError("Incomplete development reference manifest")
            receipt = json.loads(args.receipt.read_bytes())
            check_freeze_receipt(receipt, expected_protocol_sha256=args.expected_protocol_sha256)
            if not args.synthetic_development_pin and args.expected_development_pin_sha256 != receipt["approved_development_reference_sha256"]:
                raise ValueError("Unapproved production development reference; synthetic fixtures require explicit mode")
            if reference["end_exclusive"] != midnight(receipt["protocol"]["development_end_exclusive"]).isoformat() or reference["availability_lag_seconds_assumed"] != receipt["protocol"]["lag_seconds_assumed"]:
                raise ValueError("Development reference and protocol mismatch")
            data, manifest = load_snapshot(args.directory, expected_csv_sha256=args.expected_development_csv_sha256, expected_manifest=reference)
            result = freeze_model(data, receipt, expected_protocol_sha256=args.expected_protocol_sha256,
                                  expected_development_frame_sha256=frame_digest(data))
            result["development_snapshot_manifest"] = manifest
            result["development_reference"] = reference
            result["development_reference_sha256"] = args.expected_development_pin_sha256
            result["development_pin_mode"] = "synthetic_explicit" if args.synthetic_development_pin else "approved_production"
        elif args.command == "final-predict":
            import pandas as pd
            result = predict_features(json.loads(args.freeze.read_text()), pd.read_csv(args.features),
                                      expected_freeze_sha256=args.expected_freeze_sha256)
        else:
            if not args.acknowledge_unblind:
                raise ValueError("Explicit unblind acknowledgement required before reading outcomes")
            import pandas as pd
            bundle, sealed = json.loads(args.freeze.read_text()), json.loads(args.predictions.read_text())
            check_score_seals(bundle, sealed, expected_freeze_sha256=args.expected_freeze_sha256,
                              expected_prediction_sha256=args.expected_prediction_sha256, state_directory=args.state_directory)
            payload = args.outcomes.read_bytes()
            if hashlib.sha256(payload).hexdigest() != args.expected_outcome_file_sha256:
                raise ValueError("Outcome file hash mismatch")
            data = pd.read_csv(io.BytesIO(payload))
            result = score_once(bundle, sealed, data,
                                expected_freeze_sha256=args.expected_freeze_sha256, expected_prediction_sha256=args.expected_prediction_sha256,
                                expected_outcomes_sha256=frame_digest(data), state_directory=args.state_directory, acknowledge_unblind=True, report_path=args.output,
                                outcome_file_sha256=args.expected_outcome_file_sha256)
        if args.command != "final-score":
            with args.output.open("x", encoding="utf-8") as stream:
                json.dump(result, stream, indent=2, allow_nan=False)
                stream.write("\n")
        print(json.dumps({"output": str(args.output), "canonical_content_sha256": digest(result)}))
        return result
    if args.command in ("uncertainty", "protocol-receipt", "holdout-timing-preflight"):
        if args.command == "uncertainty":
            payload = args.ledger.read_bytes()
            result = paired_uncertainty(json.loads(payload), gap_policy=args.gap_policy)
            result["forecast_report_sha256"] = hashlib.sha256(payload).hexdigest()
        elif args.command == "protocol-receipt":
            result = protocol_receipt(json.loads(args.protocol.read_text()))
        else:
            result = audit_holdout_timing(json.loads(args.metadata.read_text()), json.loads(args.receipt.read_text()),
                                         expected_protocol_sha256=args.expected_protocol_sha256)
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
        print(json.dumps({"output": str(args.output)}))
        return result
    if args.command == "acquire":
        from .data import _make_exchange
        frame, quality = acquire(_make_exchange(), start=args.start, end=args.end, as_of=args.as_of)
        result = save_snapshot(args.directory, frame, start=args.start, end=args.end,
                               as_of=args.as_of, acquisition=quality)
    elif args.command == "simulate":
        frame, manifest = load_snapshot(args.directory)
        payload = args.ledger.read_bytes()
        frozen = json.loads(payload)
        if frozen.get("schema") != 1 or frozen.get("snapshot_sha256") != manifest["sha256"]:
            raise ValueError("Frozen ledger must reference this exact snapshot")
        if frozen["protocol"]["lag_seconds_assumed"] != manifest["availability_lag_seconds_assumed"]:
            raise ValueError("Ledger/snapshot availability assumption differs")
        scenarios = json.loads(args.scenarios.read_text()) if args.scenarios else DEFAULT_SCENARIOS
        result = simulate(frame, frozen["ledger"], holdout_start=frozen["protocol"]["holdout_start"],
                          as_of=args.as_of, window_start=args.window_start, terminal_open=args.terminal_open,
                          lag_seconds=manifest["availability_lag_seconds_assumed"],
                          initial_quote=args.initial_quote, scenarios=scenarios)
        result["snapshot_manifest"] = manifest
        result["forecast_report_sha256"] = hashlib.sha256(payload).hexdigest()
        result["forecast_code_sha256"] = frozen["code_sha256"]
        result["target_name"] = frozen["protocol"].get("target_name", CLOSE_TARGET)
        result["data_kind"] = frozen["source_kind"]
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
        print(json.dumps({"output": str(args.output), "accepted_signals": result["accepted_signals"]}))
        return result
    elif args.command == "ledger":
        frame, manifest = load_snapshot(args.directory)
        result = forecast_ledger(frame, holdout_start=args.holdout_start, as_of=args.as_of,
                                 lag_seconds=manifest["availability_lag_seconds_assumed"],
                                 min_train=args.min_train, test_size=args.test_size,
                                 min_calibration=args.min_calibration,
                                 snapshot_sha256=manifest["sha256"], source_kind="retrospective_public_snapshot", target=args.target)
        result["snapshot_manifest"] = manifest
        # Exclusive creation protects a prior research result from accidental replacement.
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
        print(json.dumps({"output": str(args.output), "counts": result["counts"]}))
        return result
    else:
        frame, manifest = load_snapshot(args.directory)
        dev = development_only(frame, holdout_start=args.holdout_start, as_of=args.as_of,
                               lag_seconds=manifest["availability_lag_seconds_assumed"])
        result = {"snapshot_sha256": manifest["sha256"], "holdout_start": args.holdout_start,
                  "as_of": args.as_of, "evaluation": "development only; final holdout not evaluated",
                  "backtest": walk_forward(dev, n_splits=args.splits, as_of=args.as_of)}
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()

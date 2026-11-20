"""Snapshot acquisition and development-only research entry point."""
import argparse
import hashlib
import json
from pathlib import Path

from .baseline import walk_forward
from .ledger import forecast_ledger
from .execution import DEFAULT_SCENARIOS, simulate
from .targets import CLOSE_TARGET, OPEN_TARGET
from .snapshot import acquire, development_only, load_snapshot, save_snapshot


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
    args = parser.parse_args()
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

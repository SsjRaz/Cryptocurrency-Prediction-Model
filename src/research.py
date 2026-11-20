"""Snapshot acquisition and development-only research entry point."""
import argparse
import json
from pathlib import Path

from .baseline import walk_forward
from .ledger import forecast_ledger
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
    ledger.add_argument("--holdout-start", required=True)
    ledger.add_argument("--as-of", required=True)
    ledger.add_argument("--min-train", type=int, default=80)
    ledger.add_argument("--test-size", type=int, default=30)
    ledger.add_argument("--min-calibration", type=int, default=30)
    ledger.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "acquire":
        from .data import _make_exchange
        frame, quality = acquire(_make_exchange(), start=args.start, end=args.end, as_of=args.as_of)
        result = save_snapshot(args.directory, frame, start=args.start, end=args.end,
                               as_of=args.as_of, acquisition=quality)
    elif args.command == "ledger":
        frame, manifest = load_snapshot(args.directory)
        result = forecast_ledger(frame, holdout_start=args.holdout_start, as_of=args.as_of,
                                 lag_seconds=manifest["availability_lag_seconds_assumed"],
                                 min_train=args.min_train, test_size=args.test_size,
                                 min_calibration=args.min_calibration,
                                 snapshot_sha256=manifest["sha256"], source_kind="retrospective_public_snapshot")
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

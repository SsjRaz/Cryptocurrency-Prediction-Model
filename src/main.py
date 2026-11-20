"""CLI for BTC daily direction research.

Examples:
  python -m src.main --csv data/raw/btc_usd_1d.csv --as-of 2026-01-01T00:00:00Z
  python -m src.main --mode backtest
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .baseline import latest_probability, walk_forward


def main(argv: list[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(
        description="Leakage-aware Bitcoin next-day direction baseline (research only)."
    )
    parser.add_argument("--csv", type=Path, help="Use local UTC daily OHLCV CSV rather than exchange API")
    parser.add_argument("--as-of", help="UTC cut-off timestamp for reproducible replay (ISO 8601 with offset)")
    parser.add_argument("--splits", type=int, default=5, help="Expanding time-series evaluation folds")
    parser.add_argument("--mode", choices=("both", "backtest", "forecast"), default="both")
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    args = parser.parse_args(argv)

    if args.csv is not None:
        data = pd.read_csv(args.csv)
        source = str(args.csv)
    else:
        # A previous on-disk cache can be stale by months; fetch fresh public
        # daily candles rather than reusing an outdated snapshot.
        from .data import get_btc_daily
        data = get_btc_daily(cache=False)
        source = "coinbase:BTC/USD:1d"

    cutoff = pd.Timestamp(args.as_of) if args.as_of is not None else None
    report = {
        "source": source,
        "research_only": True,
        "note": "Not investment advice. Evaluate against held-out periods and transaction costs.",
    }
    if args.mode in ("both", "backtest"):
        report["backtest"] = walk_forward(data, n_splits=args.splits, as_of=cutoff)
    if args.mode in ("both", "forecast"):
        report["forecast"] = latest_probability(data, as_of=cutoff)
    output = json.dumps(report, indent=2, allow_nan=False)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)
    return report


if __name__ == "__main__":
    main()

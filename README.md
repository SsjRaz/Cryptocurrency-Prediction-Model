# Bitcoin Direction Research Baseline

A reproducible **research-only** Bitcoin next-day direction model. It estimates
`P(next UTC daily candle closes above the latest completed daily candle)`.

The project intentionally starts with a transparent logistic-regression baseline,
rather than optimizing for an impressive but potentially leaked backtest score.
It does **not** place trades or establish a profitable trading strategy.

## Implemented

- Public Coinbase `BTC/USD` OHLCV ingestion using CCXT (existing data pipeline).
- UTC daily candle validation: rejects gaps, duplicate timestamps, invalid prices,
  negative volume and inconsistent high/low values.
- Drops the **ongoing / incomplete** UTC candle for evaluation and forecasting.
- Features from past and present completed candles only: momentum, realized
  volatility, candle body/range, moving-average deviation, and relative volume.
- Next-day UP/DOWN labels; never labels the last candle.
- Expanding-window cross-validation with a one-row embargo so feature/target
  times do not overlap training and test boundaries.
- Training-fold-only standardization and an identical-window historical-prior
  baseline; accuracy, Brier score and log loss for both models.
- A separate latest-candle probability fit using all available, fully known labels.
- Deterministic synthetic-data regression tests and GitHub Actions CI.

## Install

Python 3.11 or newer:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

On Windows, activate the virtual environment with `.venv\\Scripts\\activate`.

## Run

To download public Coinbase daily data and run a fresh analysis:

```bash
python -m src.main --mode both --output reports/btc-baseline.json
```

For a reproducible, **offline** replay from your own UTC daily OHLCV CSV
(columns: `timestamp,open,high,low,close,volume`, with a row timestamp at
**00:00 UTC candle start**):

```bash
python -m src.main --csv data/raw/btc_usd_1d.csv --as-of 2026-01-01T00:00:00Z --splits 5
```

Run only one stage with `--mode backtest` or `--mode forecast`.

The exchange fetch is uncached by default to avoid returning a prediction
based on stale local candles. If you use the underlying `src.data` cache
helpers directly, delete or refresh old CSV snapshots yourself.

## Interpretation

A feature row at 2026-01-01T00:00Z corresponds to the candle running until
2026-01-02T00:00Z. **Only after** that close is the feature row fully known.
The model predicts whether the **following** candle, which closes at
2026-01-03T00:00Z, finishes above the preceding close.

The model's probability is not a calibrated guarantee. It must be compared to
the historical-prior baseline on unseen data. Even outperforming that
baseline would not prove tradable alpha without transaction costs, realistic
execution, regime testing, and multiple truly held-out periods. Do not use
these predictions as investment advice.

## Current limitations and roadmap

1. **Historical data robustness:** Original CCXT pagination loads a multi-year
   history but does not yet track data-provider revisions, split-aware caches,
   or multi-exchange differences. Missing bars fail validation instead of being
   silently forward-filled.
2. **Time-series research:** Add an untouched final holdout, probabilistic
   calibration diagnostics, baselines such as always-up and momentum,
   and uncertainty intervals / market-regime analysis.
3. **Evaluation realism:** Add transaction costs and slippage in a separate
   paper-trading simulator; compare after-cost results to no-trade and passive
   exposure. No live order routing by default.
4. **Operational readiness:** Add explicit raw-data provenance (exchange,
   timeframe, retrieval date), stable model artifacts, drift monitoring, and
   periodic re-evaluation.
5. **Scope:** Expand to ETH/SOL only after BTC evaluation and data quality are
   credible. Avoid headline accuracy as the sole success metric.

## Project layout

```text
src/
  data.py       Exchange ingestion
  config.py     Exchange and timeframe options
  features.py   Data integrity, UTC completion, point-in-time features and labels
  baseline.py   Chronological validation, logistic model and prior baseline
  main.py       JSON-report CLI
tests/
  test_baseline.py
.github/workflows/
  tests.yml
```

## Snapshot and holdout protocol

Start with [the research protocol](docs/RESEARCH_PROTOCOL.md) and
[pre-change audit and evidence](docs/AUDIT.md). Prefer `python -m src.research`
for strict immutable snapshots and development-only replay. Existing
`python -m src.main` examples above now additionally require
`--holdout-start 2025-01-01T00:00:00Z`; all modes exclude the reserved period.
Historical candle availability is assumed, not historically verified.

The [development forecast ledger](docs/FORECAST_LEDGER.md) adds fixed purged
walk-forward blocks, past-OOF-only probability calibration, explicit warm-up,
paired baseline scoring and row-level timing via `python -m src.research ledger`.

The [offline execution protocol](docs/EXECUTION_PROTOCOL.md) adds self-financing
long/cash simulation, next-permitted-open timing, explicit fee/slippage and
same-window cash/buy-and-hold comparisons. The current forecast price horizon
has elapsed by that daily-open proxy; simulated accounting does not validate
an executable forecasting edge.

A distinct [execution-aligned target](docs/EXECUTION_ALIGNED_TARGET.md) is available
with `ledger --target execution_open_direction`. The original `close_direction`
remains the default. Results belong to separate target scopes; real uploaded-data
execution remains blocked in this Cloud executor pending supported byte delivery.

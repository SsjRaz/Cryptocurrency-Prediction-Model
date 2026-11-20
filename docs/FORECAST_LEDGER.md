# Milestone 2: frozen-fold development ledger

No final-holdout evaluation is implemented. The reserved final period remains
2025-01-01T00:00:00Z onward. Freeze the protocol and commit it before viewing
real development scores. Default training/block/calibration sizes are 80/30/30;
these are engineering defaults, not selected using market results.

## Timing and isolation

For a feature candle opening at t, assumed availability and decision are
`t + 1 day + 60 seconds`; target compares that candle close with the next
candle close, whose maturity is `t + 2 days + 60 seconds`. Historical publication
and revisions remain unverified. There is no modeled compute or execution
latency; the next milestone must add realistic executable prices.

Raw bars are truncated before features and labels using both the holdout
boundary and replay as-of. Only observations with matured outcomes by as-of
are included in this retrospective ledger. There is no current forecast or
trading interface. The bar immediately before the reserved period can close
at the boundary; no reserved candle contributes to a label.

Fold blocks start at a fixed index after the minimum training prefix and one
purged observation. Their boundaries never depend on total future sample count.
The model freezes at the first decision in the block. Only training labels
strictly matured before freeze can enter its fit; equal-time labels are purged.
All features are trailing and scaling is fitted within that training prefix.
Later rows in the block use the same frozen model. Final partial blocks are
included and identified by their actual covered end, which can extend on a
later replay without changing existing probabilities.

The calibrator is an L2 logistic mapping (C=1) of clipped raw probability logits.
Its inputs are distinct raw logistic OOF forecasts from earlier blocks only,
whose outcomes strictly matured before the current block freeze. Training-set
predictions and same-block outcomes never enter calibration. It requires 30
eligible rows and both classes. Its coefficient, intercept, historical input
bounds, count and maximum maturity are recorded. Insufficient/single-class
history falls back explicitly to the uncalibrated raw probability. A single-class
base-model training set uses the historical prior and is excluded from future
calibration input. Fallback rows are never marked as calibrated.

## Comparisons and identifiers

Each ledger row carries fold/train/test bounds, freeze, feature availability,
decision, target close/maturity, observed target, prior, persistence, neutral,
raw logistic and calibrated-or-fallback probabilities. Persistence predicts the
previous daily return sign, clipped to [1e-6,1-1e-6]; neutral is 0.5 and prior
is the mean of the same purged training labels. This is a deliberately simple,
confident persistence comparator, not an optimized momentum model.

Two populations are reported: all OOF rows (including clearly counted calibration
warm-up/fallback rows) and the subset with an actually fitted calibrator. Every
method in each population is scored on exactly the same observations. Reported
metrics are Brier score, log loss and accuracy, plus fixed ten-bin reliability
counts/means/outcome fractions and ECE. Empty paired populations return no scores.
ECE is descriptive, sample-size sensitive and not a proper scoring rule; Brier
and log loss are the primary probability measures. No significance or economic
edge is inferred from these diagnostics.

Reports include the input snapshot SHA, normalized development-data SHA, exact
source-code SHA over the ledger and its local dependencies/CLI, parameters and
numeric package versions. The output ledger contains row-level provenance under
these report identifiers; it is created exclusively rather than overwritten.
Dependency ranges remain unpinned transitively. A checksum identifies content,
not authenticity or original historical publication.

## Reproduce with permitted provider access

```sh
python -m src.research acquire data/snapshots/btc-2023 \
  --start 2023-01-01T00:00:00Z --end 2024-01-01T00:00:00Z \
  --as-of 2026-11-20T00:00:00Z
python -m src.research ledger data/snapshots/btc-2023 \
  --holdout-start 2025-01-01T00:00:00Z --as-of 2026-11-20T00:00:00Z \
  --min-train 80 --test-size 30 --min-calibration 30 \
  --output btc-2023-development-ledger.json
python -m pytest -q
```

The public acquisition was retried in this Cloud task and failed before any
candles at `coinbase GET https://api.coinbase.com/v2/currencies`, underlying DNS
`Temporary failure in name resolution`. No real data, market score, or holdout
result was produced; no restricted-access workaround was attempted. Synthetic
regression tests alone exercise the ledger. Live-provider behavior and true
point-in-time availability remain unvalidated.

Next executable milestone: obtain an allowed real development snapshot, archive
its manifest and ledger with this fixed protocol, investigate coverage and
probability diagnostics, then model execution at the first realistically
available price after decision. Add fees/slippage, turnover and position/exposure
accounting, passive/no-trade comparisons and block-based uncertainty before
freezing one honest final holdout evaluation. No profitability claim is made.

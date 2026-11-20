# Reproducible BTC research protocol

Use `python -m src.research`, the strict snapshot route. Legacy `src.main`
now requires --holdout-start and truncates before any labels or forecasts;
its legacy acquisition is not a provenance-verified route.

Before viewing market metrics, commit a dated protocol defining snapshot
identity, development dates, reserved final holdout start, target, features,
fold count, primary metric (Brier score), baselines and comparison rules.
For the first acquisition smoke test only: request 2023-01-01 through
2024-01-01 exclusive. This is development data. Reserve all data from
2025-01-01T00:00:00Z for a later frozen final evaluation. Do not download or
score the reserved period while designing the pipeline. No final-holdout
command is provided here. The boundary is procedural, not access control.

Example (explicit offsets required):

```sh
python -m src.research acquire data/snapshots/btc-2023 \
  --start 2023-01-01T00:00:00Z --end 2024-01-01T00:00:00Z \
  --as-of 2026-11-20T00:00:00Z
python -m src.research development data/snapshots/btc-2023 \
  --holdout-start 2025-01-01T00:00:00Z --as-of 2026-11-20T00:00:00Z
```

Candles are [UTC open, UTC open + 1 day). Availability is assumed at close
plus 60 seconds; provider publication latency and historical revisions are
NOT verified. Historical endpoint downloads cannot establish true historical
point-in-time availability. acquired_at is recorded separately from replay
as_of. Exact interval coverage is mandatory. Identical overlaps are counted;
conflicts, missing bars, malformed OHLCV, empty or stalled pages fail closed.
Snapshots are immutable directories with canonical CSV checksum and identity,
coverage and availability checks on reload. Keep the manifest and exact bytes
together. Hashes detect corruption, not malicious replacement of both files.

Raw development bars are selected before feature/label construction. The last
development feature with a target is therefore two opens before holdout start;
no holdout close contributes to a development label. Existing expanding folds
retain the conservative one-row gap and training-only standardization/prior.
These are repeated development diagnostics, not untouched test evidence.
The caller must not change the reserved boundary to expose final data.

Next checkpoint: store row-level out-of-fold probabilities with feature,
label-end and prediction times; prove training labels are available before
each forecast; compare logistic, historical prior and constant 0.5 using
identical rows. Any calibrator must fit exclusively to past out-of-fold
predictions whose labels have matured before its test block; retain uncalibrated
results, single-class handling and paired uncertainty. Do not fit a calibrator
on the same fold it scores. This checkpoint is not implemented yet.

Later: next executable prices after assumed availability, fees/slippage,
turnover and exposure accounting, passive/no-trade comparisons, block-based
uncertainty, then one frozen final holdout evaluation. Direction accuracy is
not evidence of trade profitability.

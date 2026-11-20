# Distinct execution-aligned research target

## Target identity and time semantics

`close_direction` remains the default original next-close target. Its label,
probability scope and one-row strict-maturity purge are preserved. New optional
`execution_open_direction` is a separate task; do not pool their metrics or treat
one target's calibration/forecast evidence as validation of the other.

For a UTC feature candle open t and assumed availability lag L, features and
decision are available at t+1 day+L. Let e be the first UTC daily open strictly
later than that decision. The new target is 1 iff `open(e+1 day) > open(e)`, else
0, including ties. For L=60 seconds, e=t+2 days and exit=t+3 days. Neither a
signal close nor an already-passed open is used. This changes the *research
question*, not the original baseline's definition. It does not prove an edge.

Entry/exit are price-event timestamps. **Label maturity is different:** this
archive provides completed daily OHLCV, not an independently timestamped open
feed. Conservatively, the exit open is considered observable only after the
entire exit candle closes plus L. Thus maturity=t+4 days+60 seconds, despite the
price horizon ending at t+3 days. This intentionally delays learning and does
not claim actual provider opening-price publication latency. Entry fills
remain hypothetical conservative daily-open timing proxies, with fees/slippage
and execution quality unverified. For lags exceeding a day, entry advances to
the next strictly later open and maturity advances accordingly; tests cover this.

Raw development bars are selected before target construction. Entry/exit opens
must both exist in those bars, and label maturity must be <= replay as-of.
No reserved candle is read for a label. With a 2025-01-01 holdout boundary,
execution-aligned exits are no later than the 2024-12-31 open. Their conservative
maturity can be 2025-01-01+L using only that completed 2024 candle; this is not a
2025 price observation or holdout score.

Training requires maturity strictly earlier than the block's decision freeze.
At L=60 seconds the aligned target therefore purges three rows, versus one for
the original target. Anchored expanding blocks start after the same minimum
number of matured training observations plus their target-specific purge.
The prior and raw logistic fit use exactly that purged prefix. Calibration
uses only distinct raw OOF scores from earlier blocks whose longer-horizon
labels strictly matured before freeze; no same-block outcomes or training
predictions. Existing warm-up and single-class fallbacks remain explicit.

Ledger rows carry target identity, entry/exit opens, retrospective entry/exit
reference prices, maturity and training/calibration bounds. New open-target
rows do not misname their exit open as label_close_utc. The simulator verifies
aligned entry equals the permitted execution open and exit is one day later;
it rejects mixed-target reports. It separately counts entries after the
original close target and entries after the selected target's exit. Retrospective
outcome prices never drive order selection.

The target describes a single entry-to-next-open direction. The simulator's
position may persist across adjacent long signals, and block models/calibrators
can remain frozen across many dates. Direction probability scoring still does
not include costs, turnover, actual fills or intraday risk. Daily-open matching
resolves the horizon mismatch in the *hypothetical proxy*, not actual trading.

## Exact uploaded-data status in this executor

The coordinating task reports a verified `btc-development.zip` containing
exactly candles.csv and manifest.json, 1827 Coinbase BTC/USD UTC daily bars from
2020-01-01 through 2024-12-31, end-exclusive 2025-01-01. This executor has **not**
received or independently verified those bytes. Expected handoff digests:

- ZIP SHA256: 7e16cf841587af7a76877d9506f1a45bec5e56c44fc66a1706f0960078b3c6f1
- CSV SHA256: 77563ae0f45a06ae3f8b63d925543978a8c05e98c9ce4156d6656f2ac3064d8e

Only the exact authorized resolved Library reference was requested, with a
consumer-local destination and the expected ZIP digest enforced by the official
transfer helper. Preparation succeeded; byte transfer failed with
`download failed: proxy tunnel HTTP 403`. No destination ZIP was installed.
No alternative storage URL, other attachment search, repeated provider
acquisition or raw-data publication was attempted. **No real-data forecast,
Brier/log-loss or cost simulation was performed here.** Do not substitute the
parent's byte verification for an execution result in this environment.

## Frozen development procedure once verified local bytes are available

Receive only the authorized file through a supported delivery route. Before
extracting, require the ZIP digest above; require exactly the two named regular
files, no duplicate names, links, directories, extra members or path traversal.
Use bounded sizes and an exclusive destination. Verify CSV digest above and
`load_snapshot` manifest/identity/coverage, then verify exact 1827 rows and
[2020-01-01,2025-01-01) coverage. Keep bytes outside the tracked repository and
never publish the raw attachment. This step has not been executed here.

Freeze defaults independently of results: minimum training 80, block size 30,
minimum calibration 30, snapshot's recorded lag, reserved boundary 2025-01-01.
Use distinct output names and preserve both target scopes. Example commands:

```sh
python -m src.research ledger /workspace/btc-authorized-input/snapshot \
  --target close_direction --holdout-start 2025-01-01T00:00:00Z \
  --as-of 2026-11-20T00:00:00Z --output /workspace/close-development.json
python -m src.research ledger /workspace/btc-authorized-input/snapshot \
  --target execution_open_direction --holdout-start 2025-01-01T00:00:00Z \
  --as-of 2026-11-20T00:00:00Z --output /workspace/open-development.json
python -m src.research simulate /workspace/btc-authorized-input/snapshot \
  --ledger /workspace/open-development.json --as-of 2026-11-20T00:00:00Z \
  --window-start 2020-04-10T00:00:00Z --terminal-open 2024-12-31T00:00:00Z \
  --output /workspace/open-execution.json
```

Repeat simulate with close-development.json and a distinct output path for a
separate diagnostic. The common predetermined 2020-04-10 through 2024-12-31 open
window permits comparison to same-window passive/no-trade, not a claim that the
two classification tasks share labels. Report each target's all-OOF and actually
calibrated paired counts and raw/calibrated/prior Brier and log loss on identical
rows within that target; preserve warm-up, calibration bins and input/code IDs.
Report all three default scenarios without selecting a winner: threshold 0.55,
fee/slippage bps (0,0), (10,5), (50,25). Include terminal wealth, costs, turnover,
exposure and open-mark drawdown for each method and both benchmarks. Treat
real-data runs as development diagnostics without significance or alpha claims.

## Validation and next milestone

Synthetic counterexamples deliberately make original close labels differ from
open-to-open labels. Tests check exact audit prices/times, three-row purge,
independent raw and past-OOF calibration reconstruction, maturity ties, delayed
lags, replay as-of, future/holdout poisoning, appended-history invariance,
deterministic warm-up, same-row scoring and simulator alignment/scope rejection.
Existing original-target tests remain green. No real-data tests occurred.

Next: deliver the verified development bytes through a permitted route and run
the frozen two-scope protocol above. Inspect development diagnostics, then
address dependency locking, uncertainty, provider publication/revision evidence
and execution assumptions. Freeze exact target, data/code, model/calibration,
selection criteria, costs, timing and final evaluation rules before opening
any 2025+ holdout. No final-holdout data or scores are present in this executor.

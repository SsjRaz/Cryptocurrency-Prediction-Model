# Locked pre-holdout protocol and matched-loss uncertainty

The committed `protocols/preholdout.json` records the research decision before
any 2025+ inspection in this task. It is **locked, not permission to evaluate**.
The proposed final window is 2025-01-01 through 2026-01-01 exclusive, with the
execution-aligned target as primary and original-close results diagnostic only.
Final model and calibrator are to be frozen strictly before the holdout begins,
with no within-holdout updates. This differs explicitly from expanding
*development* folds; it prevents final-year adaptive fitting. The final fitting
and scoring runner is not implemented in this checkpoint. Only a strict
metadata timing preflight exists; no real holdout command has been run.

## Statistical estimand and algorithm

Primary: calibrated-minus-prior daily squared probability error on identical
actually-calibrated rows. Secondary: raw-minus-prior on those same rows.
Negative mean favors the model; no conversion to strategy returns is valid.
Warm-up/raw fallback observations are excluded from **all** methods in the
paired population, with counts reported. Duplicate dates, mixed targets,
reserved outcomes, invalid probabilities/labels and outcomes not matured at
replay as-of fail closed. Neither original and aligned labels nor their scores
are pooled. Fold reports show dated counts and mean differences, not independent
replications, significance tests or a mean of unweighted fold means.

Fixed seed 20261120, 2000 replicates, non-circular overlapping moving blocks of
7, 14 and 28 contiguous UTC feature dates. Sample starts uniformly from each
permitted segment, concatenate blocks and trim the final block to original
segment length. Use the same seeded draw schedule for raw and calibrated
comparisons. Report percentile 2.5% and 97.5% quantiles, bootstrap mean/SD and
observed mean for **every** block length, without choosing a favorable interval.
The mean gives equal weight to dates, not folds or segments.

Default gap policy rejects any gap in the paired dates, including gaps created
by warm-up/fallback filtering. Optional `--gap-policy segments` resamples each
contiguous run independently at its original length, combines segment means
with original row-count weights and never bridges gaps. It conditions on
observed segment boundaries; it does not infer dependence across missing dates.
If any segment has fewer than max(20,4*block_length) rows, that block-length
interval is withheld for the whole sample. Short segments remain in the point
estimate; they are not dropped to create an attractive interval. Empty
populations return no estimate or interval. The four-block guardrail is an
engineering finite-sample minimum, not a power or coverage theorem.

Non-circular block endpoints have fewer candidate appearances, so bootstrap
means need not center on the sample mean. Percentile intervals are approximate,
conditional on the archived predictions and realized training history. They do
not refit models, correct development/model selection, guarantee stationarity,
provide uncertainty for portfolio returns or establish causal/financial claims.
Finite 2000-draw Monte Carlo variation and block-length sensitivity remain.
A full research review may reasonably conclude the available sample is inadequate.

Predeclared statistical success criterion for the future final test: primary
mean < 0 and all three 95% upper bounds < 0, with every required interval
available. Otherwise report no robust improvement; do not change block lengths
or model. Secondary comparisons, neutral/log loss/calibration diagnostics and
costs remain descriptive. This is a conservative reporting rule, not a proof
of alpha or a multiple-testing correction for past development work.

## Scripts, receipts and strict timing preflight

```sh
python -m src.research uncertainty --ledger /workspace/open-development.json \
  --output /workspace/open-development-uncertainty.json
python -m src.research protocol-receipt protocols/preholdout.json \
  --output /workspace/preholdout-receipt.json
```

The first command consumes a development ledger and writes dated paired loss
values, contiguous runs, fold summaries, all sensitivity intervals and content
hashes. Local reports are not published raw to GitHub. No real development
ledger is available to this executor, so real uncertainty was **not** computed.
The supplemental aggregates are documented separately and cannot replace losses.

The receipt content-addresses the canonical protocol JSON, relevant source
files, Python and numeric package versions. Preserve it at a reviewed immutable
commit before opening data; any subsequent source/environment/config drift
invalidates it. Full transitive dependency locking and independent audit remain
required; this receipt deliberately does not claim those are complete.

`holdout-timing-preflight` accepts timing metadata, a receipt and its independently
retained protocol hash. It rejects code/config/environment drift, non-aligned
horizons, early decisions, duplicate/gapped/out-of-period records, outcomes beyond
the frozen window, changing fit bounds and training/calibration labels that were
not strictly matured at a pre-holdout freeze. It reads no probabilities or
outcomes and generates no scores or predictions. Synthetic tests poison those
values to prove metadata-only behavior. This is not a complete final evaluator:
it does not prove a supplied model actually used its declared training data,
guarantee full planned coverage or authorize final data access.

Before a real final run: independently review the frozen target/availability,
features/logistic/scaling/calibration rules, comparators, dates, warm-up handling,
threshold, execution/terminal rules and cost assumptions; lock dependencies;
freeze scripts/reporting/success criterion with the receipt; authorize exactly
one evaluation; record the untouched snapshot identity and checksum **before**
metrics; implement/review the final fitting/scoring runner and expected complete
row coverage. Costs/passive/cash must share identical feasible dates. Never
retune after seeing holdout results, even if the result is underperformance.
No 2025+ input, acquisition, inspection or score has occurred in this executor.

This checkpoint retains a local-environment receipt at
`protocols/preholdout-receipt.json` for audit. It fingerprints these exact source
bytes and installed numeric versions. It will intentionally fail validation in
a different runtime/version set; independent review must resolve and lock the
final runtime before any holdout opening, preserving this earlier receipt as
history rather than silently replacing it.

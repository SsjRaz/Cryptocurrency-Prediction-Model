# Pre-change review, 2026-11-20

Target verified: SsjRaz/Cryptocurrency-Prediction-Model (1036813465).
Clean local `work` checkout at main 339881c39f90868c60acc561493244f474634638.
Live draft PR #1 remains open, unmerged, head e59266cf4588e1649a1c131eef1d7ae0e3d81c47.
New branch feat/point-in-time-research starts at that exact head; no other repo touched.
No AGENTS.md or repository skills found in mounted workspace or PR tree.

Baseline: Python 3.12, `/tmp/crypto-venv/bin/python -m pytest -q`: 13 passed
in 1.52s, external anyio pytest deprecation warning. Default pip installation
failed because user site is read-only; isolated /tmp venv succeeded.

PR #1 strengths: trailing features, next-day label alignment, training-only
scaling/prior, expanding folds with conservative one-row gap, synthetic tests,
offline CSV replay, Python 3.11/3.12 CI. No real-market validation demonstrated.

Findings before edits:
- acquisition drops malformed rows and silently keeps the first duplicate;
  revised/conflicting provider responses are hidden;
- empty/stalled pagination returns partial history rather than proving coverage;
- near-now stopping uses wall clock and does not establish exact interval coverage;
- cache has no identity, checksum, acquisition time or revision provenance;
- UTC parser accepts naive timestamps as UTC; sub-microsecond midnight offsets pass;
- backtest/forecast CLI has no reserved final holdout and defaults to both modes;
- no execution model, costs, calibration isolation or profitability evidence.

Existing fold metrics are development diagnostics, not a final holdout result.
A historical endpoint provides revised retrospective candles, not evidence of
historical publication times. Candle close alone is an assumed availability time.

## Acquisition evidence

Attempted the documented public Coinbase acquisition for [2023-01-01,
2024-01-01), as_of 2026-11-20T00:00:00Z. CCXT failed before OHLCV retrieval:
`ccxt.base.errors.NetworkError: coinbase GET https://api.coinbase.com/v2/currencies`,
underlying DNS `Temporary failure in name resolution`. No snapshot or real
market metrics were produced. No network workaround or alternative relay used.
Synthetic tests are logic tests only. Real-provider behavior, revisions and
publication latency remain unvalidated.

## Foundation checkpoint validation

Final local suite: 28 passed in 1.68s, Python 3.12; `git diff --check` clean.
Versions: ccxt 4.5.93, pandas 2.2.3, numpy 2.3.5,
scikit-learn 1.8.0, pytest 9.2.0. Requirements still use ranges; this is an
observed environment record, not a complete transitive dependency lock.
Added 15 synthetic regression cases: reordered pagination/equal overlap,
empty/stalled/gapped/conflicting/invalid/misaligned provider pages, close-plus-lag
availability, naive times, snapshot roundtrip/checksum/overwrite/coverage,
reserved target boundary and future-price mutation invariance, nanosecond
alignment and required CLI holdout exclusion. Existing 13 baseline tests pass
unchanged. No real-market score comparison is possible without acquired data.

Remaining coverage: real CCXT integration and live provider pagination,
provider revision timing, Python 3.11 locally, transitive dependency lock,
row-level forecast availability and past-OOF calibration. Legacy acquisition
remains available for compatibility but is explicitly outside verified snapshots.
Second checkpoint calibration is deferred until the snapshot foundation can
be exercised on permitted real development data; no holdout performance viewed.

## Milestone 2 pre-change verification and result

Reverified clean PR #2 branch and live refs: main 339881c, PR #1 e59266cf,
PR #2 ec951757; baseline suite 28 passed in 2.45s. New branch
feat/oof-forecast-ledger starts at full PR #2 SHA without changing either PR.

Adds a fixed-block OOF ledger, strict label-maturity purge, distinct prior-OOF
calibration, explicit fallbacks, paired scoring, reliability bins, code/data
hashes and timing/fold bounds. Details and commands: FORECAST_LEDGER.md.
Local final suite: 42 passed in 4.57s on Python 3.12 (same external anyio
pytest deprecation warning). Added 14 synthetic cases covering holdout/future
poisoning, appended-data forecast invariance, strict maturity ties, deterministic
warm-up, duplicate calibration observation rejection, single-class/insufficient
history fallbacks, independent calibration and raw-model reconstruction,
replay availability, invalid protocol and exclusive CLI report creation.

The first test run exposed an overly strict test expectation at the holdout
close boundary and a dtype-sensitive development hash after holdout mutation.
The boundary test now allows the last development candle to close exactly at
the boundary; the hash normalizes numeric types before serialization. Earlier
predictions and scores were already unchanged in that hash failure.

Public 2023 development acquisition retried: same api.coinbase.com DNS failure
before any OHLCV returned. No real snapshot, score comparison, final holdout
score or profitability evidence. No network workaround attempted. Protocol
uses held-out synthetic fixtures only for logic verification.

Remaining: real-provider integration and historical publication/revision
validation, full dependency locking, large-data performance, block uncertainty,
calibration stability under regime/model drift and execution economics.
Calibrator training uses probabilities from older expanding models; this
avoids leakage but does not guarantee transfer or calibration improvement.

## Milestone 3 pre-change verification and result

Reverified clean branch at 0ddeb593a87f44625a2fc6abb918311cb293a813,
prior PR/main remote refs unchanged; no new repo instructions found. Baseline
42 passed in 3.99s on Python 3.12. New branch feat/offline-execution-simulator
starts at that exact PR #3 head. No prior PR branch rewritten.

Adds an offline, quote/base self-financing simulator over frozen probabilities,
strictly subsequent daily-open proxy, shared benchmark window, explicit rejected
order statuses, transition-only costs and terminal liquidation. Default cost
scenarios are predetermined, with a common 0.55 threshold and no ranking.
Full semantics and limitations: EXECUTION_PROTOCOL.md.

Synthetic checks total 67 passing in the final local verification; 25 added cases cover deterministic toy transitions and
persistence, zero/positive/extreme costs, fee/slippage wealth reconciliation,
no trades, terminal rules, out-of-window orders, gaps/missing/duplicate orders,
causality and future mutation, exact-midnight strict timing, frozen-ledger
holdout poisoning integration and CLI snapshot binding/exclusive output, observed-outcome independence, invalid
initial cash and terminal holdout rejection.
External anyio pytest deprecation warning persists. No market scores exist.

The forecast's original target-close boundary is at the first allowed daily-open
entry under the 60-second availability assumption: entry lags decision by
86,340 seconds. This is explicitly counted/reported, not hidden with a new label
or tuned horizon. Execution-aligned forecasting remains a future design task.

User-requested strict 2020–2024 development snapshot is not yet available.
No repeated DNS probe or network workaround. Real provider/fill validation,
exchange size constraints, intraday risk, dependency locking, uncertainty and
honest untouched final evaluation remain outstanding. Final holdout unscored.

## Milestone 4: separate target and exact uploaded-file blocker

Pre-change checkout clean at PR #4 head 77b3bc8ea5ebefddb42a2861b886392af8ce8940;
remote target/main refs verified; existing 67 tests passed in 4.50s. New branch
feat/execution-aligned-target starts there, preserving all earlier branches.

Only the specifically authorized uploaded Library reference was materialized
using the skill's resolved-reference route, consumer-local destination and
expected ZIP checksum. Transfer preparation succeeded, helper download failed
`proxy tunnel HTTP 403`; exact destination ZIP is absent. No bytes were received,
extracted or verified here; **no real-data evaluation was performed**. Expected
digests and reproducible future evaluation instructions are recorded in
EXECUTION_ALIGNED_TARGET.md. Parent verification is identified as parent evidence,
not claimed as this executor's run. No URL probing, storage search, new acquisition
or publication of raw attachment occurred.

Adds optional distinct open-to-open target with explicit entry/exit prices and
conservative full-exit-candle maturity. Strict train purge grows from one to
three observations at the default lag; calibration uses longer maturity too.
Original target remains default. Simulator checks alignment and rejects mixed
scopes; reports separate original-horizon and selected-horizon expiry counts.
Validation remains synthetic only. Final holdout remains unscored.

Final local validation: 74 tests passed in 6.60s on Python 3.12; 7 added
aligned-target cases cover audit prices/times, unchanged default original target,
future/holdout poisoning, independent longer-horizon fits/calibration, delayed
lag/as-of, simulator scope/alignment rejection, append invariance/warm-up and
invalid target rejection. Same external anyio pytest deprecation warning only.
CLI help exposes explicit target selection; git diff --check clean.

## Milestone 5: uncertainty and locked pre-holdout protocol

Pre-change clean checkout at 2986742a288a4a97c3260c1985dc0f67b2551c37;
main/feature remote refs verified. Existing 74 tests passed in 6.42s. New branch
feat/preholdout-uncertainty starts at exact PR #5 head; no earlier work rewritten.

Coordinator supplied separate verified-byte offline development aggregates at
PR #5 code, documented with explicit attribution in
SUPPLEMENTAL_DEVELOPMENT_RESULTS.md. These are not this Cloud executor's results.
Here no denied transfer was retried, no ZIP obtained and no market uncertainty
computed. Both logistic variants trail prior and neutral supplied probability
scores, and the supplied strategy trails passive in each cost scenario. No
attempt to tune models/thresholds to rescue the development result was made.
Official Coinbase candle documentation was checked: open is a bucket's first
trade, not evidence of a fill exactly at the bucket timestamp.

Adds paired calibrated/raw-minus-prior daily Brier loss differences, fixed-seed
7/14/28-day non-circular moving-block intervals, fold/date audit output, strict
gap rejection or explicit independently-resampled contiguous segments, and
finite-sample refusal without dropping short segments. No confidence interval
is derived from rounded supplemental aggregates.

Committed protocols/preholdout.json locks proposed target, dates, model/fold and
calibration parameters, comparators, execution assumptions, scenarios, statistics
and a predeclared success criterion. Receipt fingerprints source/config/environment;
metadata-only holdout preflight rejects timing/fit-bound/drift violations and
never reads outcomes/probabilities. It is not a final scoring runner or access
authorization. Full dependency lock, complete final prediction-generation/coverage
verification and independent review remain prerequisites. No actual 2025+ data,
prices, prediction or score inspected or produced in this executor.

Final local verification: 101 passed in 6.92s (Python 3.12), one external anyio
pytest deprecation warning. New 27 synthetic cases cover independent block-draw
oracle, paired counts/sign, constant/zero differences, gap segmentation/short
segments, sensitivity, empty samples, duplicate/mixed/holdout/invalid outcomes,
deterministic seed, protocol drift and metadata-only invariance, late training
and calibration labels, timing/gap/frozen-fit rejection, receipt and CLI paths.
CLI receipt generated without accessing market data and retained in
protocols/preholdout-receipt.json. It records this local numeric environment;
it is not a complete transitive dependency lock or final-run authorization.

## Milestone 6: frozen final runner and verified dependency/runtime lock

Clean baseline at ff3f72fd0d7391f48ffa6478f411743bb6b2a613 verified against
remote (main unchanged); 101 tests passed in 6.89s before edits. New branch
feat/frozen-final-runner starts at exact PR #6 head; earlier PRs preserved.

Adds pre-boundary matured-label scaler/model fit, earlier-OOF-only fitted
calibrator, JSON frozen arrays, feature-only sealed predictions, hash-bound
explicit scoring, local exclusive attempt/completion records and identical
361-row final model/prior support. Original protocol dates/criteria unchanged.
Full final report includes fixed-cost same-window passive/cash diagnostics,
loss/calibration metrics, block sensitivity and the predeclared statistical
criterion. Any absence of a valid final calibrator fails closed, not fallback.

Reference environment CPython 3.12.14 Linux x86_64: pip-tools 7.7.0/pip 26.3
resolved requirements-final.lock with transitive hashes. Initial default cache
was read-only; redirected cache to /tmp and compilation succeeded. Downloaded
binary wheels with --require-hashes; installed into a clean no-system-site venv
with --no-index/--require-hashes. pip check: no broken requirements. Exact wheel
inventory/hash/size recorded in runtime/reference-wheelhouse.json; binaries
not committed. Runtime guard checks interpreter patch, platform and every pin.
Python binary/container/OS/CPU reproducibility and other platforms are not
fully locked. Normal baseline runtime skips reference-only final tests when
not matching; a dedicated exact-Python/hash-locked CI job covers the full suite.

Final locked-suite run: 119 passed in 13.69s, no warnings; git diff --check clean.
18 added cases cover independent scaler prefix, fixed parameters despite late
context/target poison, future feature/target causality, no fit in prediction or
scoring, full synthetic CLI and 361-row domains, identical primary support,
cost benchmark windows, malformed/overlapping/incomplete/empty inputs, failed
and repeated local attempts, drift/late labels, wrong hashes/overlap and runtime
lock guardrails. Existing synthetic pipeline tests remain green.

No real 2025+ acquisition, inspection, fitting, prediction or scoring occurred.
No development attachment transfer was retried or requested. The exact later
outcome interval is [2025-01-01,2025-12-31) UTC (364 candles; last open Dec 30),
with a 361-candle feature prefix through Dec 27 and existing 2024 Dec 18–31
context. No 2026 warm-up/terminal padding is necessary. See
FROZEN_FINAL_RUNNER.md for precise gates and remaining independent review.

Old receipts remain historical and are invalid for changed source; new
protocols/final-runner-receipt.json fingerprints this clean reference runtime
and final source bytes before any unblinding. Human/infrastructure protection
is still required: deleting local markers or changing directories/hashes can
bypass guards. No global one-evaluation guarantee is claimed.

Initial remote run 44812280660 at 00f7e705 failed before scheduling jobs:
plain YAML scalar contained the colon-space in --only-binary=:all:. Local
YAML parsing reproduced the exact error at workflow line 33. Corrected the
install command to a block scalar and parsed/checked both jobs locally. No
runner, dependency lock or frozen receipt source changed in this correction.

## PR #7 independent-audit blockers, before any unblinding

Continued on the existing clean feat/frozen-final-runner at c587b097, preserving
all prior commits/branches/main. Fixed two independently identified TOCTOU gaps:
final-score and load_snapshot now parse the exact authenticated byte buffer,
not a reopened CSV pathname. Added explicit independent development CSV and
reference-manifest pins; production reference is receipt-bound to the approved
2020–2024 Coinbase BTC/USD daily bytes, with exact coverage/identity/lag. An
explicit synthetic mode keeps fixture reuse separate from production approval.
The lower-level freeze API requires an expected frame identity too. Receipt and
protocol drift checks run before CLI development loading/fitting.

Changed completion lifecycle: attempt and directory durable before metrics;
requested report written exclusively and file/parent fsynced before a completion
record is created. Completion hashes exact final report bytes/content, including
raw outcome-file identity. Report write/fsync failures cannot claim completion;
attempts remain for manual recovery, with no automated rescore. Local marker
bypass and non-atomic report/completion crash windows remain documented.

New synthetic regression tests swap mutable CSV paths after authentication,
including last three outcome-only rows; verify pinned manifest fields and
self-consistent substitute rejection; require explicit non-production mode; and
inject report-write/fsync failures. No real 2025+ input was fetched, inspected,
fitted or scored; no attachment transfer/recovery workaround. Holdout remains
strictly blocked pending independent re-review and any additional audit issues.

Final locked validation after audit corrections: 131 passed in 15.62s, no
warnings. Added 12 focused regression cases; no full-runtime lock change.
Rebuilt protocols/final-runner-receipt.json and independently checked equality
to current protocol_receipt output, including the approved development-reference
hash. git diff --check clean. No real holdout execution occurred.

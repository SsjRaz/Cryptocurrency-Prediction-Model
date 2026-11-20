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

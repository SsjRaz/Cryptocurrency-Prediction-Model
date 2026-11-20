# Supplemental offline development evidence, not a Cloud reproduction

Source: coordinating Dot environment's independent offline run, reported to this
Codex Cloud task on 2026-11-20. This Cloud executor did **not** receive the ZIP or
reproduce these market metrics. Its earlier authorized transfer failed with
proxy tunnel HTTP 403; no retry was made. No raw candles or forecast ledgers are
published here. These are rounded derived aggregates supplied by the coordinator,
not newly calculated uncertainty intervals or untouched holdout results.

The coordinator reports verifying ZIP SHA256
7e16cf841587af7a76877d9506f1a45bec5e56c44fc66a1706f0960078b3c6f1 and CSV SHA256
77563ae0f45a06ae3f8b63d925543978a8c05e98c9ce4156d6656f2ac3064d8e, 1827 contiguous
Coinbase BTC/USD UTC daily candles from 2020-01-01 through 2024-12-31. It fetched
Python files from GitHub at exact PR #5 commit
2986742a288a4a97c3260c1985dc0f67b2551c37 and verified their Git blob SHAs. Reported
runtime: Python 3.12.14, numpy 2.3.5, pandas 2.2.3, scikit-learn 1.8.0. Both target
ledger and simulator commands ran in that separate executor. No 2025+ prices or
holdout were used. Per-file run output digests are not available in this handoff.

## Separate probability tasks

Supplied matched score summaries below use the actually calibrated paired
population within each target; targets must not be pooled. All-OOF/paired counts
were 1731/1671 for close direction and 1727/1667 for execution-aligned opens.

| Target | Method | Brier | Log loss |
| --- | --- | ---: | ---: |
| Original close | Prior | 0.250301 | 0.693755 |
| Original close | Raw logistic | 0.251046 | 0.695628 |
| Original close | Calibrated logistic | 0.250878 | 0.695198 |
| Aligned open | Prior | 0.250463 | 0.694081 |
| Aligned open | Raw logistic | 0.252125 | 0.697541 |
| Aligned open | Calibrated logistic | 0.251166 | 0.695512 |
| Each target | Neutral | 0.250000 | 0.693147 |

Calibrated-minus-prior Brier is positive in both supplied summaries (+0.000577
close, +0.000703 aligned), so calibration does not establish improvement over
the prior. Raw logistic also performs worse than prior and neutral. Calibration
improves on raw in these development aggregates, not enough to beat the simple
comparators. There is no evidence here of a predictive edge. Rounded aggregate
scores cannot produce paired time-series uncertainty; row-level differences
must be analyzed separately with the committed procedure, without retuning.

## Hypothetical accounting, not fill evidence

Common window: 2020-04-10 open through terminal liquidation at 2024-12-31 open.
Threshold 0.55; rates below are per executed side. No best-scenario selection.

| Fee/slippage bps | Aligned calibrated cumulative return | Passive cumulative return |
| --- | ---: | ---: |
| 0 / 0 | +237.05% | +1169.95% |
| 10 / 5 | +192.72% | +1166.15% |
| 50 / 25 | +66.54% | +1151.05% |

At 10/5, the aligned calibrated strategy had 94 executed sides and 56.86%
maximum open-mark drawdown; historical-prior strategy return was +148.63% and
cash 0%. The model lagged passive in every supplied cost scenario. Positive
hypothetical returns during a rising BTC window are not alpha, significance or
profitability evidence. Outcomes, drawdowns and costs remain proxies.

All 1726 accepted original-close signals entered at/after their original target
expiry; none of 1726 aligned signals entered at/after its aligned target exit.
This separates the price horizons but does not solve fill quality. Coinbase
[defines candle open as the first trade within the bucket](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles),
not a guaranteed executable price exactly at bucket start. Precise first-trade
times, quote spreads, available sizes and actual latency are absent from daily
candles. Daily bucket timestamps in code are timing conventions, not measured
trade or publication timestamps.

Development underperformance is a valid result. No models, calibration settings,
thresholds or reported cost scenarios were changed to rescue it. Next work is
uncertainty and protocol audit, not selecting a favorable development headline.

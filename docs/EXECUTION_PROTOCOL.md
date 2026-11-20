# Milestone 3: offline cost and execution accounting

This simulator is development-only research. No live orders, credentials,
funds or profitability claims. It consumes frozen forecast-ledger probabilities;
it does not choose a model, threshold or cost scenario based on returns. The
2020–2024 development snapshot requested from the user is pending. Only synthetic
logic/arithmetic verification has been performed; the failed provider DNS probe
was not repeated in this milestone.

## Timing proxy and forecast-horizon limitation

A signal can act only at the first UTC daily-bar open **strictly after** its
decision, which must be no earlier than candle availability. Even a decision
exactly at an open waits until the following open. There is no fill at the
signal close or an open already passed. This is a conservative timing proxy,
not evidence of fill quality, available liquidity, intraday latency or best
execution. Slippage is a configurable assumption, not a measured quantity.

For the existing label with feature open t, decision is t+1 day+60 seconds,
while the first daily-open proxy is t+2 days. That is 86,340 seconds after
decision, **at the original target-close boundary**: the direction forecast's
price horizon has elapsed before simulated entry. The report quantifies this
for every accepted signal and counts entries at/after original target close.
The label itself matures another 60 seconds later under the assumption, but
this does not extend its price horizon. No accuracy metric is repackaged as an
executable return. Simulation returns describe a delayed use of the frozen
signal, not a valid same-horizon trading interpretation. Execution-aligned
relabeling and finer time resolution remain future research, with a newly
frozen development protocol before tuning.

## Shared window and explicit order rules

The user supplies a UTC-midnight start and terminal open, strictly before the
reserved holdout. Every strategy and both benchmarks use those exact inclusive
open dates. The start is flat quote cash, with no position inherited from
signals mapping before the window. Signals are mapped by clock, not by searching
for the next observed row: missing/gapped, duplicate or out-of-bound window bars
fail closed, never jump to a later available price. Accepted signal feature
candles must exist in the development snapshot. Ambiguous duplicate signals
mapping to the same execution open fail closed. Naive times, premature decisions
and invalid accepted probabilities are rejected.

Orders before/after the window are recorded without execution. At the terminal
open any base position is sold with the same sell fee/slippage; a signal mapped
to that open is explicitly marked terminal_no_new_position. No terminal entry
or zero-duration round trip is allowed. A terminal open must exist and its
candle must be completed under the snapshot availability filter (conservative
input requirement, despite using only its open). Final holdout bars and orders
are not scored. The CLI inherits the holdout boundary from the frozen ledger,
checks exact snapshot SHA and availability assumption, and retains the snapshot
manifest, original report byte hash and code identifiers.

## Self-financing quote/base book

Long if p > threshold, otherwise cash. Ties are cash. All cash is spent only on
a cash-to-long transition; all base is sold only on a long-to-cash transition.
A repeated long signal does not rebalance or incur cost. Missing signal dates
preserve the existing position. No leverage, shorts, lending yield, cash interest,
funding or external cash flows are modeled. Fractional base quantities are
allowed; exchange tick/lot/minimum size rules remain unmodeled.

With reference open P, fee fraction f, adverse slippage s and initial quote Q:

- Buy fill is P(1+s), quantity is Q/[P(1+s)(1+f)], fee is quantity*fill*f;
  quote becomes zero.
- Sell fill is P(1-s), quote received is quantity*fill*(1-f); base becomes zero.
- Slippage is recorded separately as quantity*abs(fill-P). Fees use executed
  notional. Both sides, including terminal sales, pay their respective costs.

Each side records before/after quote and base, quantity, reference/fill and
separate costs. Reference-price wealth loss equals fee plus slippage; slippage
is not deducted again after already entering the fill price. Rates must be
finite, nonnegative and strictly below 10,000 bps individually. This guardrail
does not imply that extreme accepted costs are realistic. Starting capital
must be positive/finite. Balances are checked for finiteness and nonnegativity.

Equity is quote+base*reference open **after that open's trades**, including
terminal costs. Drawdown includes starting cash in its peak and uses these
post-trade open marks only; it cannot measure intraday drawdown. Turnover is the
sum of absolute executed reference notional on all sides divided by initial
quote. Invested interval fraction counts open-to-next-open intervals held long;
it is not leveraged notional exposure. No annualized Sharpe or significance
claim is made. The report includes full holdings/equity paths for audit.

No-trade holds quote throughout. Buy-and-hold buys at the declared start and
liquidates at the same terminal open, using identical rates/accounting. This
is a common window comparison, not an attempt to give the benchmark the same
signal timing. Calibrated-probability strategy includes the milestone-2 explicit
raw fallback during calibration warm-up; the source calibrated flag is retained.

## Predetermined scenarios and reproduction

Defaults hold threshold fixed at 0.55, with (fee, slippage) bps of (0,0), (10,5)
and (50,25). These are illustrative engineering assumptions, not estimates of a
particular account's costs. Results remain in input order without ranking or
best-of selection. Alternative named scenarios are supplied as a JSON list
before viewing returns, e.g.:

```json
[{"name":"fixed_protocol","fee_bps":10,"slippage_bps":5,"threshold":0.55}]
```

After a strict snapshot and its ledger exist (see earlier protocol commands):

```sh
python -m src.research simulate data/snapshots/btc-2023 \
  --ledger btc-2023-development-ledger.json \
  --as-of 2026-11-20T00:00:00Z \
  --window-start 2023-04-08T00:00:00Z \
  --terminal-open 2023-12-31T00:00:00Z \
  --initial-quote 1000 --output btc-2023-execution.json
python -m pytest -q
```

The example dates are a predetermined development smoke window, not selected
from returns. Output creation is exclusive. Archive exact snapshot, forecast
report, scenario file and execution report together. Hashes identify content,
not authenticity. The public 2023 acquisition commands remain documented in
FORECAST_LEDGER.md; no market data is bundled here.

## Before opening an untouched final holdout

Freeze and commit: snapshot/provider/revision/availability rules and checksums;
reserved dates and access procedure; forecast target and executable horizon;
features/model/calibration/fold schedule and warm-up policy; signal/threshold
selection rule; order timing/latency and fill/size assumptions; fee/slippage
scenarios; position/terminal rules; identical benchmark windows; primary metrics,
paired population, uncertainty method and decision criteria. Review real
*development-only* ledger and cost diagnostics first. Freeze exact code and
dependencies, account for all development experiments, and specify one final
evaluation with no retuning after observing it. Provider publication timing,
execution-quality validation, block uncertainty and a full dependency lock are
still missing. The current final holdout remains unscored.

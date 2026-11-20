"""Offline long/cash accounting at strictly subsequent daily-open timing proxies."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .ledger import METHODS
from .snapshot import DAY, development_only, midnight, utc

# Predetermined engineering scenarios, not a search or ranking.
DEFAULT_SCENARIOS = (
    {"name": "zero_cost", "fee_bps": 0., "slippage_bps": 0., "threshold": .55},
    {"name": "illustrative_cost", "fee_bps": 10., "slippage_bps": 5., "threshold": .55},
    {"name": "stress_cost", "fee_bps": 50., "slippage_bps": 25., "threshold": .55},
)


def _account(bars, targets, *, capital, fee, slip):
    quote, base, peak, drawdown = capital, 0., capital, 0.
    trades, equity = [], []
    fees, slippage, notional = 0., 0., 0.
    invested_intervals = 0
    for i, bar in enumerate(bars.itertuples()):
        reference = float(bar.open)
        terminal = i == len(bars) - 1
        target = 0 if terminal else targets.get(bar.timestamp, int(base > 0))
        side = "buy" if target and base == 0 else "sell" if not target and base > 0 else None
        if side:
            before_quote, before_base = quote, base
            fill = reference * (1 + slip if side == "buy" else 1 - slip)
            quantity = quote / (fill * (1 + fee)) if side == "buy" else base
            fee_quote = quantity * fill * fee
            slip_quote = quantity * abs(fill - reference)
            if side == "buy":
                base, quote = quantity, 0.
            else:
                quote, base = quote + quantity * fill - fee_quote, 0.
            fees += fee_quote
            slippage += slip_quote
            notional += quantity * reference
            trades.append({"open_utc": bar.timestamp.isoformat(), "side": side,
                           "terminal_liquidation": terminal, "reference_open": reference,
                           "proxy_fill": fill, "quantity": quantity, "fee_quote": fee_quote,
                           "slippage_quote": slip_quote, "quote_before": before_quote,
                           "base_before": before_base, "quote_after": quote, "base_after": base})
        value = quote + base * reference
        peak = max(peak, value)
        drawdown = max(drawdown, 1 - value / peak)
        if not terminal and base > 0:
            invested_intervals += 1
        equity.append({"open_utc": bar.timestamp.isoformat(), "quote": quote, "base": base,
                       "reference_open": reference, "equity": value,
                       "position": int(base > 0), "drawdown": 1 - value / peak})
        if not np.isfinite(value) or quote < 0 or base < 0:
            raise ValueError("Non-finite or negative self-financing balance")
    return {"initial_quote": capital, "terminal_quote": quote, "terminal_base": base,
            "net_return": quote / capital - 1, "max_open_drawdown": drawdown,
            "fee_quote_total": fees, "slippage_quote_total": slippage,
            "reference_notional_turnover": notional / capital,
            "invested_interval_fraction": invested_intervals / (len(bars) - 1),
            "executed_sides": len(trades), "trades": trades, "equity": equity}


def simulate(frame, frozen_ledger, *, holdout_start, as_of, window_start, terminal_open,
             lag_seconds=60, initial_quote=1000., scenarios=DEFAULT_SCENARIOS):
    """Identical inclusive open windows; terminal always liquidates, never enters.

    Forecast targets are NOT execution returns: the next permitted open is
    generally already at the original target-close boundary.
    """
    if not np.isfinite(initial_quote) or initial_quote <= 0:
        raise ValueError("Initial quote must be positive and finite")
    start, terminal, boundary = midnight(window_start), midnight(terminal_open), midnight(holdout_start)
    if not start < terminal < boundary:
        raise ValueError("Window must end strictly before holdout and after start")
    dev = development_only(frame, holdout_start=boundary, as_of=as_of, lag_seconds=lag_seconds)
    bars = dev.loc[dev.timestamp.between(start, terminal)].reset_index(drop=True)
    expected = pd.Series(pd.date_range(start, terminal, freq="D"), name="timestamp")
    if not bars.timestamp.equals(expected):
        raise ValueError("Missing or out-of-bound window opens; no gap bridging")
    target_names = {row.get("target_name", "close_direction") for row in frozen_ledger}
    if len(target_names) > 1:
        raise ValueError("Mixed target scopes cannot share an execution report")
    orders, accepted, occupied = [], [], set()
    for row in frozen_ledger:
        feature = midnight(row["feature_open_utc"])
        if feature >= boundary:
            # Never consume probabilities or outcomes from the reserved period.
            raise ValueError("Ledger contains reserved holdout feature")
        available, decision = utc(row["candle_available_utc"]), utc(row["decision_utc"])
        if available < feature + DAY + pd.Timedelta(seconds=lag_seconds) or decision < available:
            raise ValueError("Signal decision precedes assumed availability")
        permitted = decision.floor("D") + DAY
        status = "accepted"
        if permitted < start:
            status = "before_window"
        elif permitted > terminal:
            status = "after_window"
        elif permitted == terminal:
            status = "terminal_no_new_position"
        if status == "accepted":
            if feature not in set(dev.timestamp):
                raise ValueError("Accepted signal feature missing from development snapshot")
            if permitted in occupied:
                raise ValueError("Multiple signals map to the same execution open")
            occupied.add(permitted)
            for method in METHODS:
                if not np.isfinite(row[method]) or not 0 <= row[method] <= 1:
                    raise ValueError("Invalid frozen probability")
            accepted.append((permitted, row))
        label_close = utc(row.get("label_exit_utc", feature + 2 * DAY))
        if status == "accepted" and row.get("target_name") == "execution_open_direction":
            if utc(row["label_entry_utc"]) != permitted or label_close != permitted + DAY:
                raise ValueError("Execution target and permitted opens disagree")
        orders.append({"feature_open_utc": feature.isoformat(), "decision_utc": decision.isoformat(),
                       "candle_available_utc": available.isoformat(),
                       "permitted_open_utc": permitted.isoformat(), "status": status,
                       "decision_to_open_seconds": (permitted - decision).total_seconds(),
                       "target_exit_utc": label_close.isoformat(),
                       "original_target_close_utc": (feature + 2 * DAY).isoformat(),
                       "target_expired_at_entry": permitted >= label_close,
                       "target_name": row.get("target_name", "close_direction"),
                       "probabilities": {m: float(row[m]) for m in METHODS} if status == "accepted" else None,
                       "source_fold": row.get("fold"), "calibrated": row.get("calibrated")})
    results, names = [], set()
    for scenario in scenarios:
        name = scenario["name"]
        fee, slip, threshold = (float(scenario[k]) for k in ("fee_bps", "slippage_bps", "threshold"))
        if name in names or not name:
            raise ValueError("Scenario names must be distinct")
        names.add(name)
        if not all(np.isfinite([fee, slip, threshold])) or not (0 <= fee < 10000 and 0 <= slip < 10000 and 0 <= threshold <= 1):
            raise ValueError("Invalid costs or threshold; fee and slippage must be below 10000 bps")
        strategies = {}
        for method in METHODS:
            targets = {stamp: int(row[method] > threshold) for stamp, row in accepted}
            strategies[method] = _account(bars, targets, capital=initial_quote, fee=fee / 10000, slip=slip / 10000)
        strategies["no_trade"] = _account(bars, {}, capital=initial_quote, fee=fee / 10000, slip=slip / 10000)
        strategies["buy_and_hold"] = _account(bars, {start: 1}, capital=initial_quote, fee=fee / 10000, slip=slip / 10000)
        results.append({**scenario, "strategies": strategies})
    if not results:
        raise ValueError("At least one predetermined scenario required")
    return {"research_only": True, "data_kind": "offline; inspect input provenance",
            "execution_proxy": "conservative timing proxy: first daily open strictly after decision; no fill-quality evidence",
            "window_start": start.isoformat(), "terminal_open": terminal.isoformat(),
            "holdout_start": boundary.isoformat(), "as_of": utc(as_of).isoformat(),
            "initial_quote": initial_quote, "lag_seconds_assumed": lag_seconds,
            "threshold_rule": "long if p > threshold, otherwise cash; persist between signals",
            "terminal_rule": "liquidate at terminal open with sell fee/slippage; reject new positions",
            "window_opens": len(bars), "accepted_signals": len(accepted),
            "orders": orders, "scenarios": results,
            "target_name": next(iter(target_names), "unspecified_empty_ledger"),
            "horizon_limitation": {"accepted_at_or_after_original_target_close": sum(stamp >= utc(row["feature_open_utc"]) + 2 * DAY for stamp, row in accepted),
                                   "accepted_at_or_after_target_exit": sum(stamp >= utc(row.get("label_exit_utc", utc(row["feature_open_utc"]) + 2 * DAY)) for stamp, row in accepted),
                                   "accepted_signals": len(accepted),
                                   "interpretation": "target scope is separate; classification scores do not include costs or establish executable returns"},
            "market_window_sha256": hashlib.sha256(bars.to_csv(index=False).encode()).hexdigest(),
            "frozen_ledger_sha256": hashlib.sha256(json.dumps(frozen_ledger, sort_keys=True, allow_nan=False).encode()).hexdigest(),
            "execution_code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}

"""Toy accounting checks, never market-performance evidence."""
import copy

import numpy as np
import pandas as pd
import pytest

from src.execution import simulate
from src.ledger import METHODS, forecast_ledger

ZERO = ({"name": "toy", "fee_bps": 0., "slippage_bps": 0., "threshold": .5},)


@pytest.fixture
def market():
    times = pd.date_range("2022-12-30", periods=12, tz="UTC")
    prices = np.array([90, 90, 90, 100, 120, 80, 110, 100, 100, 100, 100, 100.])
    return pd.DataFrame({"timestamp": times, "open": prices, "close": prices,
                         "high": prices * 1.01, "low": prices * .99, "volume": 10.})


def signal(feature, p):
    stamp = pd.Timestamp(feature)
    decision = stamp + pd.Timedelta(days=1, seconds=60)
    return {"feature_open_utc": stamp.isoformat(), "candle_available_utc": decision.isoformat(),
            "decision_utc": decision.isoformat(), **{m: p for m in METHODS}}


def run(market, rows, **kwargs):
    return simulate(market, rows, holdout_start="2023-01-10T00:00:00Z",
                    as_of="2023-01-11T00:01:00Z", window_start="2023-01-02T00:00:00Z",
                    terminal_open="2023-01-05T00:00:00Z", **kwargs)


def strategy(result, name="logistic_raw"):
    return result["scenarios"][0]["strategies"][name]


def test_zero_cost_transitions_persistence_and_benchmarks(market):
    rows = [signal("2022-12-31T00:00:00Z", .8), signal("2023-01-02T00:00:00Z", .2)]
    result = run(market, rows, scenarios=ZERO)
    account = strategy(result)
    assert [t["open_utc"] for t in account["trades"]] == ["2023-01-02T00:00:00+00:00", "2023-01-04T00:00:00+00:00"]
    assert account["terminal_quote"] == pytest.approx(800.)
    assert account["terminal_base"] == 0
    assert account["reference_notional_turnover"] == pytest.approx(1.8)
    assert account["max_open_drawdown"] == pytest.approx(1 - 800 / 1200)
    assert account["invested_interval_fraction"] == pytest.approx(2 / 3)
    assert strategy(result, "no_trade")["terminal_quote"] == 1000
    assert strategy(result, "buy_and_hold")["terminal_quote"] == pytest.approx(1100)
    assert result["horizon_limitation"]["accepted_at_or_after_original_target_close"] == 2
    assert result["orders"][0]["decision_to_open_seconds"] == 86340
    assert all(len(a["equity"]) == 4 for a in result["scenarios"][0]["strategies"].values())


def test_fee_and_slippage_separate_self_financing_and_terminal(market):
    scenario = ({"name": "arithmetic", "fee_bps": 100, "slippage_bps": 200, "threshold": .5},)
    result = run(market, [signal("2022-12-31T00:00:00Z", .8)], scenarios=scenario)
    account = strategy(result)
    quantity = 1000 / (100 * 1.02 * 1.01)
    terminal = quantity * 110 * .98 * .99
    assert account["terminal_quote"] == pytest.approx(terminal)
    assert account["fee_quote_total"] == pytest.approx(quantity * (102 + 107.8) * .01)
    assert account["slippage_quote_total"] == pytest.approx(quantity * (2 + 2.2))
    assert len(account["trades"]) == 2 and account["trades"][-1]["terminal_liquidation"]
    for trade in account["trades"]:
        if trade["side"] == "buy":
            assert trade["quote_before"] == pytest.approx(trade["quantity"] * trade["proxy_fill"] + trade["fee_quote"])
        else:
            assert trade["quote_after"] - trade["quote_before"] == pytest.approx(trade["quantity"] * trade["proxy_fill"] - trade["fee_quote"])
    assert account == strategy(result, "buy_and_hold")


def test_no_trades_outside_orders_and_terminal_rejection(market):
    rows = [signal("2022-12-30T00:00:00Z", .9), signal("2022-12-31T00:00:00Z", .5),
            signal("2023-01-03T00:00:00Z", .9), signal("2023-01-04T00:00:00Z", .9)]
    result = run(market, rows, scenarios=ZERO)
    assert [r["status"] for r in result["orders"]] == ["before_window", "accepted", "terminal_no_new_position", "after_window"]
    assert strategy(result)["executed_sides"] == 0
    assert strategy(result)["terminal_quote"] == 1000
    assert strategy(result)["max_open_drawdown"] == 0
    assert strategy(run(market, [], scenarios=ZERO))["terminal_quote"] == 1000


@pytest.mark.parametrize("bad", ["gap", "missing_terminal", "duplicate", "early_decision", "probability", "holdout"])
def test_fail_closed(market, bad):
    rows = [signal("2022-12-31T00:00:00Z", .8)]
    if bad == "gap":
        market = market.drop(index=4)
    elif bad == "missing_terminal":
        market = market.iloc[:6]
    elif bad == "duplicate":
        rows *= 2
    elif bad == "early_decision":
        rows[0]["decision_utc"] = "2023-01-01T00:00:00Z"
    elif bad == "probability":
        rows[0]["prior"] = float("nan")
    else:
        rows.append(signal("2023-01-10T00:00:00Z", .8))
    with pytest.raises(ValueError):
        run(market, rows, scenarios=ZERO)


@pytest.mark.parametrize("field,value", [("fee_bps", -1), ("fee_bps", 10000), ("slippage_bps", 10000), ("slippage_bps", float("nan")), ("threshold", 1.1)])
def test_cost_guardrails(market, field, value):
    scenario = {**ZERO[0], field: value}
    with pytest.raises(ValueError):
        run(market, [], scenarios=[scenario])


def test_extreme_valid_cost_stays_finite_unlevered(market):
    scenario = ({"name": "extreme", "fee_bps": 9999., "slippage_bps": 9999., "threshold": .5},)
    account = strategy(run(market, [signal("2022-12-31T00:00:00Z", .9)], scenarios=scenario))
    assert 0 <= account["terminal_quote"] < 1
    assert all(r["base"] >= 0 and r["quote"] >= 0 and np.isfinite(r["equity"]) for r in account["equity"])


def test_strictly_after_even_exact_midnight(market):
    row = signal("2022-12-31T00:00:00Z", .9)
    row["decision_utc"] = "2023-01-02T00:00:00Z"
    result = run(market, [row], scenarios=ZERO)
    assert result["orders"][0]["permitted_open_utc"] == "2023-01-03T00:00:00+00:00"


def test_future_price_and_probability_poisoning(market):
    rows = [signal("2022-12-31T00:00:00Z", .9), signal("2023-01-02T00:00:00Z", .2)]
    original = run(market, rows, scenarios=ZERO)
    changed = market.copy()
    changed.loc[changed.timestamp >= "2023-01-04", ["open", "high", "low", "close"]] *= 10
    other_rows = copy.deepcopy(rows)
    for m in METHODS:
        other_rows[1][m] = .9
    other = run(changed, other_rows, scenarios=ZERO)
    assert strategy(original)["equity"][:2] == strategy(other)["equity"][:2]
    assert strategy(original)["trades"][0] == strategy(other)["trades"][0]


def test_frozen_ledger_holdout_integration():
    n = 450
    prices = 100 * np.exp(np.cumsum(.01 * np.sin(np.arange(n))))
    market = pd.DataFrame({"timestamp": pd.date_range("2023-01-01", periods=n, tz="UTC"),
                           "open": prices, "close": prices, "high": prices * 1.01,
                           "low": prices * .99, "volume": 10.})
    report = forecast_ledger(market, holdout_start="2024-01-01T00:00:00Z", as_of="2025-01-01T00:00:00Z")
    def evaluate(frame):
        return simulate(frame, report["ledger"], holdout_start="2024-01-01T00:00:00Z",
                        as_of="2025-01-01T00:00:00Z", window_start="2023-04-08T00:00:00Z",
                        terminal_open="2023-12-31T00:00:00Z", scenarios=ZERO)
    original = evaluate(market)
    market.loc[market.timestamp >= "2024-01-01", ["open", "high", "low", "close"]] = np.nan
    assert original == evaluate(market)
    assert original["accepted_signals"] > 200
    assert original["orders"][-1]["status"] == "after_window"


def test_side_costs_equal_reference_wealth_loss(market):
    result = run(market, [signal("2022-12-31T00:00:00Z", .9)], scenarios=[
        {"name": "check", "fee_bps": 17., "slippage_bps": 9., "threshold": .5}])
    for trade in strategy(result)["trades"]:
        before = trade["quote_before"] + trade["base_before"] * trade["reference_open"]
        after = trade["quote_after"] + trade["base_after"] * trade["reference_open"]
        assert before - after == pytest.approx(trade["fee_quote"] + trade["slippage_quote"])


def test_simulator_cli_snapshot_binding_and_exclusive_output(market, tmp_path, monkeypatch, capsys):
    import json
    from src import research
    frozen = {"schema": 1, "snapshot_sha256": "synthetic-fixture", "code_sha256": "test-code",
              "source_kind": "synthetic", "protocol": {"holdout_start": "2023-01-10T00:00:00Z", "lag_seconds_assumed": 60},
              "ledger": [signal("2022-12-31T00:00:00Z", .9)]}
    manifest = {"sha256": "synthetic-fixture", "availability_lag_seconds_assumed": 60}
    monkeypatch.setattr(research, "load_snapshot", lambda path: (market, manifest))
    input_file, output = tmp_path / "ledger.json", tmp_path / "simulation.json"
    input_file.write_text(json.dumps(frozen))
    monkeypatch.setattr("sys.argv", ["research", "simulate", "unused-synthetic-fixture", "--ledger", str(input_file),
                                   "--as-of", "2023-01-11T00:01:00Z", "--window-start", "2023-01-02T00:00:00Z",
                                   "--terminal-open", "2023-01-05T00:00:00Z", "--output", str(output)])
    result = research.main()
    assert result["data_kind"] == "synthetic"
    assert json.loads(output.read_text())["forecast_code_sha256"] == "test-code"
    with pytest.raises(FileExistsError):
        research.main()
    frozen["snapshot_sha256"] = "different"
    input_file.write_text(json.dumps(frozen))
    with pytest.raises(ValueError, match="exact snapshot"):
        research.main()


def test_observed_forecast_outcomes_do_not_drive_orders(market):
    row = {**signal("2022-12-31T00:00:00Z", .9), "target_up": 1}
    first = run(market, [row], scenarios=ZERO)
    row["target_up"] = 0
    second = run(market, [row], scenarios=ZERO)
    assert first["scenarios"] == second["scenarios"]


@pytest.mark.parametrize("capital", [0, -1, float("nan")])
def test_invalid_initial_cash(market, capital):
    with pytest.raises(ValueError, match="Initial quote"):
        run(market, [], initial_quote=capital, scenarios=ZERO)


def test_terminal_cannot_enter_reserved_holdout(market):
    with pytest.raises(ValueError, match="Window"):
        simulate(market, [], holdout_start="2023-01-05T00:00:00Z", as_of="2023-01-11T00:01:00Z",
                 window_start="2023-01-02T00:00:00Z", terminal_open="2023-01-05T00:00:00Z")

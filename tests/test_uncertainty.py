"""Synthetic paired-loss and date-block inference checks, no market inference."""
import copy

import numpy as np
import pandas as pd
import pytest

from src.uncertainty import moving_block_means, paired_uncertainty


def report(n=300):
    rows = []
    for i, t in enumerate(pd.date_range("2023-01-01", periods=n, tz="UTC")):
        rows.append({"feature_open_utc": t.isoformat(), "label_exit_utc": (t + pd.Timedelta(days=3)).isoformat(),
                     "label_maturity_utc": (t + pd.Timedelta(days=4, seconds=60)).isoformat(),
                     "target_name": "execution_open_direction", "fold": i // 30 + 1, "calibrated": True,
                     "target_up": 0, "prior": .5, "logistic_raw": .6, "logistic_calibrated": .6})
    return {"protocol": {"target_name": "execution_open_direction", "holdout_start": "2025-01-01T00:00:00Z", "as_of": "2024-12-31T00:00:00Z"}, "ledger": rows}


def test_constant_loss_oracle_and_paired_counts():
    source = report()
    source["ledger"][0]["calibrated"] = False
    result = paired_uncertainty(source, replicates=100)
    assert result == paired_uncertainty(source, replicates=100)
    assert result["population"] == {"input_oof": 300, "paired": 299, "excluded_warmup_or_fallback": 1}
    assert sum(r["rows"] for r in result["folds"]) == 299
    for comparison in result["comparisons"].values():
        assert comparison["mean_daily_brier_difference"] == pytest.approx(.11)
        for block in comparison["block_sensitivity"]:
            assert block["ci95"] == pytest.approx([.11, .11])
    assert [r["block_length_days"] for r in result["comparisons"]["raw_minus_prior"]["block_sensitivity"]] == [7, 14, 28]


def test_independent_contiguous_block_resampling_oracle():
    values = np.sin(np.arange(80) * .08)
    n, length, replicates, seed = len(values), 7, 100, 92
    starts = np.random.default_rng(seed).integers(0, n - length + 1, size=(replicates, int(np.ceil(n / length))))
    expected = [np.concatenate([values[s:s + length] for s in row])[:n].mean() for row in starts]
    np.testing.assert_allclose(moving_block_means(values, block_length=length, replicates=replicates, seed=seed), expected)
    assert not np.array_equal(expected, moving_block_means(values, block_length=length, replicates=replicates, seed=seed + 1))


def test_gaps_rejected_or_segmented_without_dropping_short_samples():
    source = report()
    for row in source["ledger"][151:]:
        row["logistic_calibrated"] = .4
        row["logistic_raw"] = .4
    del source["ledger"][150]
    with pytest.raises(ValueError, match="Gap"):
        paired_uncertainty(source)
    result = paired_uncertainty(source, gap_policy="segments", replicates=100)
    assert [s["rows"] for s in result["segments"]] == [150, 149]
    expected = (.11 * 150 - .09 * 149) / 299
    for item in result["comparisons"]["raw_minus_prior"]["block_sensitivity"]:
        assert item["ci95"] == pytest.approx([expected, expected])
    source["ledger"] = source["ledger"][:151]
    result = paired_uncertainty(source, gap_policy="segments", replicates=100)
    assert result["comparisons"]["raw_minus_prior"]["mean_daily_brier_difference"] is not None
    assert all(r["status"] == "insufficient_contiguous_sample" for r in result["comparisons"]["raw_minus_prior"]["block_sensitivity"])


def test_finite_samples_empty_and_block_sensitivity():
    result = paired_uncertainty(report(80), replicates=100)
    statuses = [r["status"] for r in result["comparisons"]["raw_minus_prior"]["block_sensitivity"]]
    assert statuses == ["computed", "computed", "insufficient_contiguous_sample"]
    empty = paired_uncertainty(report(0), replicates=100)
    assert empty["comparisons"]["raw_minus_prior"]["mean_daily_brier_difference"] is None
    assert empty["folds"] == []


@pytest.mark.parametrize("case", ["duplicate", "holdout", "mixed", "nan", "unmatured", "invalid_y"])
def test_input_integrity(case):
    source = report()
    if case == "duplicate":
        source["ledger"].append(source["ledger"][0])
    elif case == "holdout":
        source["ledger"][0]["feature_open_utc"] = "2025-01-01T00:00:00Z"
    elif case == "mixed":
        source["ledger"][0]["target_name"] = "close_direction"
    elif case == "nan":
        source["ledger"][0]["prior"] = np.nan
    elif case == "unmatured":
        source["ledger"][0]["label_maturity_utc"] = "2026-01-01T00:00:00Z"
    else:
        source["ledger"][0]["target_up"] = 2
    with pytest.raises(ValueError):
        paired_uncertainty(source)


def test_fold_reordering_invariance_and_loss_pairing():
    source = report()
    first = paired_uncertainty(source, replicates=100)
    source["ledger"].reverse()
    other = paired_uncertainty(source, replicates=100)
    assert first["comparisons"] == other["comparisons"]
    assert first["folds"] == other["folds"]
    # Same y is used on both sides. Changing both probabilities identically
    # leaves exactly zero paired differences, irrespective of label balance.
    for i, row in enumerate(source["ledger"]):
        row["prior"] = row["logistic_calibrated"] = row["logistic_raw"] = .2 + .1 * (i % 5)
        row["target_up"] = i % 2
    result = paired_uncertainty(source, replicates=100)
    assert result["comparisons"]["calibrated_minus_prior"]["mean_daily_brier_difference"] == 0


@pytest.mark.parametrize("options", [{"replicates": 99}, {"seed": -1}, {"block_lengths": (0,)}, {"block_lengths": (7, 7)}, {"gap_policy": "bridge"}])
def test_invalid_protocol(options):
    with pytest.raises(ValueError):
        paired_uncertainty(report(), **options)

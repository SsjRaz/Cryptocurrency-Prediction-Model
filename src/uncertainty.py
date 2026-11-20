"""Matched daily Brier loss uncertainty; no target pooling or strategy selection."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .snapshot import DAY, midnight, utc

BLOCK_LENGTHS = (7, 14, 28)
SEED = 20261120


def moving_block_means(values, *, block_length, replicates, seed):
    """Non-circular overlapping contiguous blocks, truncate last draw to n.

    No time points are dropped. Edges appear in fewer candidate blocks; the
    finite-sample moving-block distribution need not center at the sample mean.
    """
    values = np.asarray(values, dtype=float)
    if values.ndim != 1 or not np.isfinite(values).all():
        raise ValueError("Loss vector must be one-dimensional and finite")
    if block_length < 1 or len(values) < block_length or replicates < 2:
        raise ValueError("Invalid block length, sample or replicate count")
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, len(values) - block_length + 1,
                          size=(replicates, int(np.ceil(len(values) / block_length))))
    indices = (starts[..., None] + np.arange(block_length)).reshape(replicates, -1)[:, :len(values)]
    return values[indices].mean(axis=1)


def paired_uncertainty(report, *, block_lengths=BLOCK_LENGTHS, replicates=2000,
                       seed=SEED, gap_policy="reject"):
    """Primary calibrated-minus-prior and secondary raw-minus-prior, same rows.

    Negative difference favors model; this conditional development diagnostic
    does not correct for prior research, model selection or regime changes.
    """
    if gap_policy not in ("reject", "segments") or replicates < 100 or seed < 0:
        raise ValueError("Invalid uncertainty protocol")
    if not block_lengths or len(set(block_lengths)) != len(block_lengths) or any(type(b) is not int or b < 1 for b in block_lengths):
        raise ValueError("Block lengths must be distinct positive integers")
    boundary = midnight(report["protocol"]["holdout_start"])
    cutoff = utc(report["protocol"]["as_of"])
    target = report["protocol"]["target_name"]
    paired = []
    seen = set()
    for row in report["ledger"]:
        stamp = midnight(row["feature_open_utc"])
        # No final-holdout outcome is accepted by this development command.
        if stamp >= boundary or utc(row["label_exit_utc"]) > boundary or (target == "execution_open_direction" and utc(row["label_exit_utc"]) >= boundary):
            raise ValueError("Reserved holdout outcome in development uncertainty input")
        if row["target_name"] != target:
            raise ValueError("Mixed target scopes")
        if stamp in seen:
            raise ValueError("Duplicate daily forecast")
        seen.add(stamp)
        if not row["calibrated"]:
            continue
        if utc(row["label_maturity_utc"]) > cutoff:
            raise ValueError("Outcome not matured by replay as-of")
        if row["target_up"] not in (0, 1):
            raise ValueError("Target must be binary")
        y = int(row["target_up"])
        p = [row[k] for k in ("prior", "logistic_raw", "logistic_calibrated")]
        if not np.isfinite(p).all() or not all(0 <= v <= 1 for v in p):
            raise ValueError("Invalid paired probabilities")
        paired.append({"date": stamp.isoformat(), "fold": row["fold"],
                       "raw_minus_prior": (p[1] - y) ** 2 - (p[0] - y) ** 2,
                       "calibrated_minus_prior": (p[2] - y) ** 2 - (p[0] - y) ** 2})
    paired.sort(key=lambda r: r["date"])
    segments = []
    for row in paired:
        if not segments or utc(row["date"]) - utc(segments[-1][-1]["date"]) != DAY:
            segments.append([])
        segments[-1].append(row)
    if gap_policy == "reject" and len(segments) > 1:
        raise ValueError("Gap in paired dates; refusing to bridge gaps")
    comparisons = {}
    for key in ("calibrated_minus_prior", "raw_minus_prior"):
        values = np.array([r[key] for r in paired])
        sensitivity = []
        for length in block_lengths:
            item = {"block_length_days": length, "segment_sizes": [len(s) for s in segments]}
            # Four non-overlapping block lengths per segment is a protocol
            # guardrail, not a theorem establishing statistical adequacy.
            if not segments or any(len(s) < max(20, 4 * length) for s in segments):
                item.update(status="insufficient_contiguous_sample", ci95=None)
            else:
                total = np.zeros(replicates)
                for i, segment in enumerate(segments):
                    means = moving_block_means([r[key] for r in segment], block_length=length,
                                               replicates=replicates,
                                               seed=np.random.SeedSequence([seed, length, i]))
                    total += means * len(segment) / len(paired)
                interval = np.quantile(total, [.025, .975], method="linear")
                item.update(status="computed", ci95=interval.tolist(),
                            bootstrap_mean=float(total.mean()),
                            bootstrap_sd=float(total.std(ddof=1)))
            sensitivity.append(item)
        comparisons[key] = {"rows": len(values), "mean_daily_brier_difference": float(values.mean()) if len(values) else None,
                            "block_sensitivity": sensitivity}
    folds = []
    for fold in sorted({r["fold"] for r in paired}):
        rows = [r for r in paired if r["fold"] == fold]
        folds.append({"fold": fold, "rows": len(rows), "first_date": rows[0]["date"], "last_date": rows[-1]["date"],
                      **{key: float(np.mean([r[key] for r in rows])) for key in comparisons}})
    return {"target_name": target, "scope": "development; paired actually-calibrated rows only",
            "population": {"input_oof": len(report["ledger"]), "paired": len(paired),
                           "excluded_warmup_or_fallback": len(report["ledger"]) - len(paired)},
            "method": "non-circular moving-block percentile interval; paired daily losses",
            "seed": seed, "replicates": replicates, "confidence": .95, "gap_policy": gap_policy,
            "segments": [{"first_date": s[0]["date"], "last_date": s[-1]["date"], "rows": len(s)} for s in segments],
            "comparisons": comparisons, "folds": folds, "daily_matched_loss": paired,
            "ledger_content_sha256": hashlib.sha256(json.dumps(report, sort_keys=True, allow_nan=False).encode()).hexdigest(),
            "uncertainty_code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "limits": "Conditional on frozen predictions; no refitting/selection correction, no profitability inference, no stationarity guarantee"}

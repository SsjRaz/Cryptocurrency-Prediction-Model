"""Strict public daily acquisition and immutable, checksum-verified offline input."""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import pandas as pd

from .features import prepare_daily

DAY = pd.Timedelta(days=1)


def utc(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError("Timestamp must be timezone aware")
    return stamp.tz_convert("UTC")


def midnight(value):
    stamp = utc(value)
    if stamp != stamp.floor("D"):
        raise ValueError("Boundary must be UTC midnight")
    return stamp


def acquire(exchange, *, start, end, as_of, limit=200, lag_seconds=60):
    """Exact [start,end) coverage; equal overlaps allowed, conflicts fail closed.

    lag_seconds is a conservative assumption, not verified publication latency.
    end is the close boundary of the final requested candle.
    """
    start, end, cutoff = midnight(start), midnight(end), utc(as_of)
    if start >= end or lag_seconds < 0 or limit < 1:
        raise ValueError("Invalid acquisition interval, lag or limit")
    if end + pd.Timedelta(seconds=lag_seconds) > cutoff:
        raise ValueError("Requested candles not yet assumed available")
    cursor, stop = start.value // 10**6, end.value // 10**6
    rows, pages, overlaps = {}, 0, 0
    while cursor < stop:
        batch = exchange.fetch_ohlcv("BTC/USD", timeframe="1d", since=cursor, limit=limit)
        pages += 1
        if not batch:
            raise ValueError("Empty page before coverage complete")
        stamps = []
        for row in batch:
            if len(row) != 6 or isinstance(row[0], bool) or int(row[0]) != row[0]:
                raise ValueError("Malformed provider row")
            timestamp = int(row[0])
            stamp = pd.to_datetime(timestamp, unit="ms", utc=True)
            if stamp != stamp.floor("D"):
                raise ValueError("Provider candle is not UTC midnight")
            stamps.append(timestamp)
            if start.value // 10**6 <= timestamp < stop:
                if timestamp in rows:
                    if rows[timestamp] != list(row):
                        raise ValueError("Conflicting duplicate provider candle")
                    overlaps += 1
                rows[timestamp] = list(row)
        next_cursor = max(stamps) + 86400000
        if next_cursor <= cursor:
            raise ValueError("Pagination made no progress")
        cursor = next_cursor
    frame = pd.DataFrame(list(rows.values()), columns=["timestamp", "open", "high", "low", "close", "volume"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    frame = prepare_daily(frame)
    expected = pd.date_range(start, end, freq="D", inclusive="left")
    if not frame.timestamp.equals(pd.Series(expected, name="timestamp")):
        raise ValueError("Incomplete requested coverage")
    return frame, {"pages": pages, "identical_overlaps": overlaps}


def save_snapshot(directory, frame, *, start, end, as_of, lag_seconds=60, acquisition=None):
    """Create a new directory; never overwrite an existing snapshot."""
    frame = prepare_daily(frame)
    start, end, cutoff = midnight(start), midnight(end), utc(as_of)
    expected = pd.Series(pd.date_range(start, end, freq="D", inclusive="left"), name="timestamp")
    if not frame.timestamp.equals(expected) or lag_seconds < 0 or end + pd.Timedelta(seconds=lag_seconds) > cutoff:
        raise ValueError("Snapshot coverage or availability mismatch")
    payload = frame.to_csv(index=False).encode()
    manifest = {"schema": 1, "source": "coinbase", "symbol": "BTC/USD", "timeframe": "1d",
                "start": start.isoformat(), "end_exclusive": end.isoformat(),
                "as_of": cutoff.isoformat(), "acquired_at": pd.Timestamp.now(tz="UTC").isoformat(),
                "availability_lag_seconds_assumed": lag_seconds,
                "historical_publication_verified": False, "rows": len(frame),
                "sha256": hashlib.sha256(payload).hexdigest(), "acquisition": acquisition}
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=False)
    (path / "candles.csv").write_bytes(payload)
    (path / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def load_snapshot(directory, *, expected_csv_sha256=None, expected_manifest=None):
    path = Path(directory)
    manifest = json.loads((path / "manifest.json").read_text())
    payload = (path / "candles.csv").read_bytes()
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    if expected_csv_sha256 is not None and actual_sha256 != expected_csv_sha256:
        raise ValueError("Independently pinned snapshot CSV mismatch")
    if expected_manifest is not None:
        for key, value in expected_manifest.items():
            if manifest.get(key) != value:
                raise ValueError(f"Reference manifest mismatch: {key}")
    if actual_sha256 != manifest["sha256"]:
        raise ValueError("Snapshot checksum mismatch")
    if (manifest["schema"], manifest["source"], manifest["symbol"], manifest["timeframe"]) != (1, "coinbase", "BTC/USD", "1d"):
        raise ValueError("Unsupported snapshot identity")
    # Parse exactly the bytes authenticated above; never reopen a mutable path.
    raw = pd.read_csv(io.BytesIO(payload))
    if list(raw.columns) != ["timestamp", "open", "high", "low", "close", "volume"]:
        raise ValueError("Unexpected snapshot CSV schema")
    frame = prepare_daily(raw)
    expected = pd.Series(pd.date_range(midnight(manifest["start"]), midnight(manifest["end_exclusive"]), freq="D", inclusive="left"), name="timestamp")
    if len(frame) != manifest["rows"] or not frame.timestamp.equals(expected):
        raise ValueError("Snapshot coverage mismatch")
    utc(manifest["acquired_at"])
    if manifest.get("historical_publication_verified") is not False:
        raise ValueError("Unsupported publication provenance")
    lag = manifest["availability_lag_seconds_assumed"]
    if lag < 0 or midnight(manifest["end_exclusive"]) + pd.Timedelta(seconds=lag) > utc(manifest["as_of"]):
        raise ValueError("Snapshot availability mismatch")
    return frame, manifest


def development_only(frame, *, holdout_start, as_of, lag_seconds=60):
    """Split raw bars BEFORE labels/features so boundary targets cannot cross."""
    boundary, cutoff = midnight(holdout_start), utc(as_of)
    if lag_seconds < 0:
        raise ValueError("Negative availability lag")
    times = pd.to_datetime(frame["timestamp"], utc=True)
    available = times + DAY + pd.Timedelta(seconds=lag_seconds) <= cutoff
    development = frame.loc[(times < boundary) & available].copy()
    return prepare_daily(development)

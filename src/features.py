"""Leakage-aware features and targets for UTC daily Bitcoin candles.

A row timestamp is the UTC candle OPEN. Features are available after that
candle closes; the label describes the close of the following daily candle.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_COLUMNS = (
    "return_1d",
    "return_3d",
    "return_7d",
    "volatility_7d",
    "volatility_14d",
    "range_pct",
    "body_pct",
    "volume_ratio_7d",
    "distance_sma_7d",
)


def prepare_daily(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate, sort and standardize a continuous sequence of UTC 1d bars."""
    required = {"timestamp", "open", "high", "low", "close", "volume"}
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing OHLCV columns: {sorted(missing)}")
    if frame.empty:
        raise ValueError("No daily OHLCV rows provided")

    data = frame.loc[:, ["timestamp", "open", "high", "low", "close", "volume"]].copy()
    data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors="raise")
    for name in ("open", "high", "low", "close", "volume"):
        data[name] = pd.to_numeric(data[name], errors="raise")

    if data["timestamp"].duplicated().any():
        raise ValueError("Duplicate daily candle timestamps")
    data = data.sort_values("timestamp").reset_index(drop=True)

    if not (data["timestamp"].dt.hour.eq(0) &
            data["timestamp"].dt.minute.eq(0) &
            data["timestamp"].dt.second.eq(0) &
            data["timestamp"].dt.microsecond.eq(0)).all():
        raise ValueError("Daily candle timestamps must start at 00:00 UTC")
    if (data["timestamp"].diff().dropna() != pd.Timedelta(days=1)).any():
        raise ValueError("Missing or irregular UTC daily candles; refusing to bridge gaps")

    numeric = data[["open", "high", "low", "close", "volume"]].to_numpy(dtype=float)
    if not np.isfinite(numeric).all():
        raise ValueError("OHLCV contains NaN or infinite values")
    if (data[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC prices must be positive")
    if (data["volume"] < 0).any():
        raise ValueError("Volume must be non-negative")
    if (data["high"] < data[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError("High is lower than another OHLC price")
    if (data["low"] > data[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError("Low is higher than another OHLC price")
    return data


def completed_daily(frame: pd.DataFrame, *, as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """Exclude the current incomplete candle before training or forecasting."""
    data = prepare_daily(frame)
    cutoff = pd.Timestamp.now(tz="UTC") if as_of is None else pd.Timestamp(as_of)
    if cutoff.tzinfo is None:
        raise ValueError("as_of must be timezone aware")
    cutoff = cutoff.tz_convert("UTC")
    completed = data.loc[data["timestamp"] + pd.Timedelta(days=1) <= cutoff].copy()
    if completed.empty:
        raise ValueError("No completed UTC daily candles")
    return completed.reset_index(drop=True)


def feature_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Build features only from the current and earlier completed OHLCV rows."""
    data = prepare_daily(frame)
    close = data["close"]
    volume = data["volume"]
    daily_returns = np.log(close / close.shift(1))

    result = pd.DataFrame({"timestamp": data["timestamp"]})
    result["return_1d"] = daily_returns
    result["return_3d"] = np.log(close / close.shift(3))
    result["return_7d"] = np.log(close / close.shift(7))
    result["volatility_7d"] = daily_returns.rolling(7).std()
    result["volatility_14d"] = daily_returns.rolling(14).std()
    result["range_pct"] = (data["high"] - data["low"]) / close
    result["body_pct"] = (close - data["open"]) / data["open"]
    result["volume_ratio_7d"] = volume / volume.shift(1).rolling(7).mean()
    result["distance_sma_7d"] = close / close.rolling(7).mean() - 1
    return result.replace([np.inf, -np.inf], np.nan)


def labeled_daily(frame: pd.DataFrame) -> pd.DataFrame:
    """Next UTC daily close direction; never assign a label to the last candle."""
    data = prepare_daily(frame)
    features = feature_frame(data)
    next_close = data["close"].shift(-1)
    features["target_up"] = (next_close > data["close"]).where(next_close.notna())
    features = features.dropna(subset=[*FEATURE_COLUMNS, "target_up"]).reset_index(drop=True)
    features["target_up"] = features["target_up"].astype(int)
    return features

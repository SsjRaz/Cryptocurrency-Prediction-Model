# src/data.py

from __future__ import annotations

import os
import time
from typing import Optional
import datetime as dt
from .config import YEARS_OF_HISTORY


import ccxt
import pandas as pd

from .config import EXCHANGE_ID, SYMBOL, FETCH_LIMIT, SLEEP_SECONDS, RAW_DATA_DIR


def _near_now_delta(timeframe: str) -> dt.timedelta:
    """
    Define how close to 'now' we stop paginating for each timeframe.
    """
    if timeframe == "1h":
        return dt.timedelta(hours=2)
    if timeframe == "1d":
        return dt.timedelta(days=1)
    if timeframe.endswith("h") and timeframe[:-1].isdigit():
        hours = int(timeframe[:-1])
        return dt.timedelta(hours=max(2, hours * 2))
    if timeframe.endswith("d") and timeframe[:-1].isdigit():
        days = int(timeframe[:-1])
        return dt.timedelta(days=max(1, days))
    return dt.timedelta(hours=2)


def _make_exchange() -> ccxt.Exchange:
    """
    Create a CCXT exchange instance with rate limiting enabled.
    Public OHLCV does not require API keys.
    """
    exchange_class = getattr(ccxt, EXCHANGE_ID)
    exchange = exchange_class({
        "enableRateLimit": True,
        # Can Add options later if needed
    })
    return exchange


def _ohlcv_to_df(ohlcv: list[list]) -> pd.DataFrame:
    """
    Convert CCXT OHLCV rows into a clean DataFrame.
    CCXT OHLCV format: [ms, open, high, low, close, volume]
    """
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp_ms", "open", "high", "low", "close", "volume"],
    )

    # Convert to UTC timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True)
    df = df.drop(columns=["timestamp_ms"])

    # Ensure numeric
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with any NaNs
    df = df.dropna().reset_index(drop=True)

    # Sort & dedupe by timestamp
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)

    return df


def fetch_ohlcv_full(
    timeframe: str,
    symbol: str = SYMBOL,
    limit: int = FETCH_LIMIT,
    sleep_seconds: float = SLEEP_SECONDS,
    cache_path: Optional[str] = None,
    debug: bool = False,
) -> pd.DataFrame:
    """
    Fetch OHLCV data from the configured exchange via CCXT, paginating from ~YEARS_OF_HISTORY ago until near “now”.

    - timeframe: "1h" or "1d"
    - symbol: e.g., "BTC/USDT"
    - cache_path: if provided and file exists, loads from cache instead of fetching
    - debug: if True, prints batch sizes and timestamps as we paginate
    """
    def _log(message: str) -> None:
        if debug:
            print(f"[fetch_ohlcv_full] {message}")

    if cache_path and os.path.exists(cache_path):
        df = pd.read_csv(cache_path)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"]).reset_index(drop=True)
        return df

    exchange = _make_exchange()

    all_rows: list[list] = []
    since: Optional[int] = None  # None means "most recent" for many exchanges

    # Calculate start timestamp for YEARS_OF_HISTORY years ago, currently set at 3 years
    start = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=365 * YEARS_OF_HISTORY)
    since = int(start.timestamp() * 1000)
    near_now_delta = _near_now_delta(timeframe)

    _log(f"start fetching {symbol} timeframe={timeframe} since={pd.to_datetime(since, unit='ms', utc=True)} limit={limit}")

    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not batch:
            _log("no rows returned; stopping")
            break

        first_ts = dt.datetime.fromtimestamp(batch[0][0] / 1000, tz=dt.timezone.utc)
        last_ts = dt.datetime.fromtimestamp(batch[-1][0] / 1000, tz=dt.timezone.utc)
        _log(f"fetched {len(batch)} rows: {first_ts} -> {last_ts}")

        all_rows.extend(batch)

        last_ms = batch[-1][0]
        next_since = last_ms + 1
        last_dt = dt.datetime.fromtimestamp(last_ms / 1000, tz=dt.timezone.utc)

        # If we didn't advance, stop
        if next_since <= since:
            _log("stopping because the last candle timestamp did not advance")
            break

        now = dt.datetime.now(dt.timezone.utc)
        if last_dt >= now - near_now_delta:
            _log(f"stopping because we are near 'now': last candle {last_dt}, now {now}")
            break

        since = next_since
        time.sleep(sleep_seconds)

    df = _ohlcv_to_df(all_rows)

    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        df.to_csv(cache_path, index=False)

    return df


def get_btc_hourly(cache: bool = True) -> pd.DataFrame:
    cache_path = os.path.join(RAW_DATA_DIR, "btc_usdt_1h.csv") if cache else None
    return fetch_ohlcv_full(timeframe="1h", cache_path=cache_path)


def get_btc_daily(cache: bool = True) -> pd.DataFrame:
    cache_path = os.path.join(RAW_DATA_DIR, "btc_usdt_1d.csv") if cache else None
    return fetch_ohlcv_full(timeframe="1d", cache_path=cache_path)

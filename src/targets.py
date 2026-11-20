"""Distinct research targets; daily open observations use full-bar availability."""
import pandas as pd

from .features import FEATURE_COLUMNS, feature_frame, labeled_daily
from .snapshot import DAY

CLOSE_TARGET = "close_direction"
OPEN_TARGET = "execution_open_direction"


def target_data(dev, *, target, lag_seconds):
    lag = pd.Timedelta(seconds=lag_seconds)
    if target == CLOSE_TARGET:
        data = labeled_daily(dev)
        data["label_entry"] = data.timestamp + DAY
        data["label_exit"] = data.timestamp + 2 * DAY
        data["maturity"] = data.timestamp + 2 * DAY + lag
        return data, 1
    if target != OPEN_TARGET:
        raise ValueError("Unknown research target")
    # Features at t are available t+1d+lag. The next strictly later open is
    # t+(2+floor(lag/1d)) days. No already-passed daily open may be used.
    entry_days = 2 + int(lag // DAY)
    result = feature_frame(dev)
    entry = dev.open.shift(-entry_days)
    exit_price = dev.open.shift(-(entry_days + 1))
    result["target_up"] = (exit_price > entry).where(entry.notna() & exit_price.notna())
    result["label_entry"] = result.timestamp + entry_days * DAY
    result["label_exit"] = result.timestamp + (entry_days + 1) * DAY
    result["label_entry_reference_open"] = entry
    result["label_exit_reference_open"] = exit_price
    # We have archived daily OHLCV, not a timestamped intraday open feed.
    # Conservatively wait for the entire exit candle + assumed provider lag.
    result["maturity"] = result["label_exit"] + DAY + lag
    result = result.dropna(subset=[*FEATURE_COLUMNS, "target_up"]).reset_index(drop=True)
    result["target_up"] = result.target_up.astype(int)
    return result, entry_days + 1

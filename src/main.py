# src/main.py

from __future__ import annotations

from .data import get_btc_hourly, get_btc_daily


def main() -> None:
    hourly = get_btc_hourly(cache=True)
    daily = get_btc_daily(cache=True)

    print("Hourly:")
    print(hourly.head(3))
    print(hourly.tail(3))
    print("rows:", len(hourly), "range:", hourly["timestamp"].min(), "->", hourly["timestamp"].max())
    print()

    print("Daily (UTC candles):")
    print(daily.head(3))
    print(daily.tail(3))
    print("rows:", len(daily), "range:", daily["timestamp"].min(), "->", daily["timestamp"].max())


if __name__ == "__main__":
    main()

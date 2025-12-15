# src/config.py

EXCHANGE_ID = "coinbase"
SYMBOL = "BTC/USD"

TIMEFRAMES = {
    "hourly": "1h",
    "daily": "1d",
}

YEARS_OF_HISTORY = 3

DEFAULT_FETCH_LIMIT = 1000
MAX_OHLCV_LIMITS = {
    "coinbase": 300,
    "kraken": 720,   # based on observations
    "binance": 1000
}

# Per-exchange limit with a safe default fallback
FETCH_LIMIT = MAX_OHLCV_LIMITS.get(EXCHANGE_ID, DEFAULT_FETCH_LIMIT)

# polite rate-limit padding (seconds)
SLEEP_SECONDS = 0.2

# cache paths
RAW_DATA_DIR = "data/raw"

# Crypto Prediction Model (Restart) — V1

This repository is a clean restart of my crypto prediction project to rebuild it with a clear, testable architecture.

## V1 Goal (BTC Only)
Predict whether Bitcoin’s price direction will be **UP or DOWN over the next 24 hours**, and output a **probability** (P(up)).

## Label Definition (Daily Close)
Because crypto trades 24/7, “daily close” is defined using **UTC daily candles** from the data provider:
- UP (1) if next day UTC close > current day UTC close
- DOWN (0) otherwise

## Current Scope
- BTC only (ETH/SOL will be added after V1 is stable)
- Start with a simple baseline model (Logistic Regression)
- Use time-based evaluation (train on earlier data, test on later data)

## Planned Future Additions
- Stronger models (XGBoost / LightGBM)
- More market features (volatility, momentum, volume spikes, etc.)
- Sentiment features (Twitter/Reddit) after the core pipeline works

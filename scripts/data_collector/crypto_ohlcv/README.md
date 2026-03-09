# Crypto OHLCV Collector (Stage 1)

This collector extends qlib's existing crypto data collection path to produce a backtest-oriented OHLCV contract.

## Current scope

- Sources: Binance Spot REST API, OKX Spot REST API
- Frequency: `1d` (default) and `1min`
- Fields: `open/high/low/close/volume/money/factor/vwap/trade_count`
- Output symbol namespace: `<EXCHANGE>_<symbol>`
  - e.g. `BINANCE_BTCUSDT`, `OKX_BTC_USDT`

## Data contract targets

### Spot baseline

- `open, high, low, close, volume, money, factor`
- `vwap, trade_count`
- `symbol, date, exchange`

### Perp extension (next phase)

- `funding_rate, open_interest, mark_price, index_price, basis`
- `taker_buy_base, taker_buy_quote`
- `next_funding_time`

## Usage

### Binance

```bash
python collector.py download_data \
  --source_dir ~/.qlib/crypto_ohlcv/source/1d \
  --start 2020-01-01 \
  --end 2024-12-31 \
  --interval 1d \
  --exchange binance \
  --quote_asset USDT
```

### OKX

```bash
python collector.py download_data \
  --source_dir ~/.qlib/crypto_ohlcv/source/1d \
  --start 2020-01-01 \
  --end 2024-12-31 \
  --interval 1d \
  --exchange okx \
  --quote_asset USDT
```

### Normalize

```bash
python collector.py normalize_data \
  --source_dir ~/.qlib/crypto_ohlcv/source/1d \
  --normalize_dir ~/.qlib/crypto_ohlcv/normalize/1d \
  --interval 1d
```

## Notes

- Binance and OKX requests use pagination loops to avoid truncating long history ranges.
- OKX candle API does not provide per-bar trade count in this endpoint; `trade_count` is left empty for OKX in stage-1.
- This stage focuses on spot OHLCV contract compatibility. Perp metadata (funding/OI/mark/index) will be added in the next stage.

# qlib-crypto Factor Mining Guide

## 1. Build qlib crypto dataset

1) collect & normalize OHLCV
2) (optional) collect perp metadata and merge
3) one-shot build + dump

```bash
python -m qlib_crypto.data.pipeline \
  --normalize_dir ~/.qlib/crypto_ohlcv/normalize/1d \
  --qlib_dir ~/.qlib/crypto_data \
  --exchange binance \
  --freq day
```

## 2. Start factor mining

```bash
python qlib_crypto/examples/scripts/factor_mining_crypto.py \
  --provider_uri ~/.qlib/crypto_data \
  --start 2020-01-01 \
  --end 2024-12-31
```

## 3. Train + backtest with qrun

```bash
qrun qlib_crypto/examples/workflow_config_lightgbm_alpha158_crypto.yaml
qrun qlib_crypto/examples/crypto_perp_daily.yaml
```

## 4. Recommended next features

- funding z-score deciles
- basis mean-reversion signals
- OI momentum and crowding
- liquidity masks and stablecoin-neutralized cross-section


## 5. 1min realtime-style workflow

```bash
qrun qlib_crypto/examples/crypto_spot_1min.yaml
```

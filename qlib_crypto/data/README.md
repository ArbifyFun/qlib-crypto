# qlib_crypto.data helpers

## Build calendar/instruments from normalized CSV

```bash
python -m qlib_crypto.data.builder \
  --source_dir ~/.qlib/crypto_ohlcv/normalize/1d \
  --calendar_path ~/.qlib/crypto_ohlcv/qlib/calendars/day.txt \
  --instruments_path ~/.qlib/crypto_ohlcv/qlib/instruments/all.txt \
  --exchange binance
```

This is intended as a pre-step before `scripts/dump_bin.py`.

## One-shot build + dump

```bash
python -m qlib_crypto.data.pipeline \
  --normalize_dir ~/.qlib/crypto_ohlcv/normalize/1d \
  --qlib_dir ~/.qlib/crypto_data \
  --exchange binance \
  --freq day
```

## Dataset healthcheck

```bash
python -m qlib_crypto.data.healthcheck --qlib_dir ~/.qlib/crypto_data --freq day
```

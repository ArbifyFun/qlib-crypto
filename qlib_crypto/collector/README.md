# qlib_crypto.collector

## Modules

- `crypto_ohlcv`: spot OHLCV collection entry (`Run`)
- `crypto_meta`: perp metadata collector (`CryptoMetaCollector`) for
  - Binance: funding history, open interest history
  - OKX: funding history, open interest snapshot

## Example

```python
from qlib_crypto.collector.crypto_meta import CryptoMetaCollector

c = CryptoMetaCollector()
funding = c.collect_binance_funding("BTCUSDT", start_time="2024-01-01", end_time="2024-03-01")
oi = c.collect_binance_open_interest("BTCUSDT", period="1d")
meta = c.merge_meta_frames([funding, oi])
```


## Incremental update

```bash
python -m qlib_crypto.collector.incremental \
  --source_dir ~/.qlib/crypto_ohlcv/source/1d \
  --exchange binance \
  update --end 2025-01-01
```


## Reliability

Collectors use retry/backoff HTTP sessions by default for transient API errors (429/5xx).

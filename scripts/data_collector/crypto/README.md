# Crypto Data Collector

This collector relies on [ccxt](https://github.com/ccxt/ccxt) to request OHLCV data
from exchanges (default: OKX). It supports 1m/1h/1d intervals and converts the raw
data into Qlib format with helpers from `qlib.data.storage`.

## Requirements

```bash
pip install -r requirements.txt
```

## Environment configuration

Public endpoints do not require authentication. To increase rate limits or access
private endpoints, set the API key, secret and passphrase as environment variables:

```bash
export OKX_API_KEY="your_key"
export OKX_API_SECRET="your_secret"
export OKX_API_PASSPHRASE="your_passphrase"
```

`CryptoCollector` enables ccxt's built-in rate limiter and defaults to a 1-second
delay between requests.

## Usage

### Download and normalize

```bash
# download OHLCV for BTC-USDT
python collector.py download_data --symbols BTC-USDT \
    --source_dir ~/.qlib/crypto/source/1d --start 2020-01-01 --end 2020-12-31 --interval 1d

# normalize
python collector.py normalize_data --source_dir ~/.qlib/crypto/source/1d \
    --normalize_dir ~/.qlib/crypto/normalize/1d --interval 1d

# convert to Qlib format
python collector.py dump_to_qlib --normalize_dir ~/.qlib/crypto/normalize/1d \
    --qlib_dir ~/.qlib/qlib_data/crypto --interval 1d
```

The `--interval` argument supports `1m`, `1h` and `1d`.

### Cron example

Run the collector every day at 00:00 UTC:

```cron
0 0 * * * python collector.py download_data --symbols BTC-USDT \
    --source_dir ~/.qlib/crypto/source/1d --interval 1d && \
    python collector.py normalize_data --source_dir ~/.qlib/crypto/source/1d \
        --normalize_dir ~/.qlib/crypto/normalize/1d --interval 1d && \
    python collector.py dump_to_qlib --normalize_dir ~/.qlib/crypto/normalize/1d \
        --qlib_dir ~/.qlib/qlib_data/crypto --interval 1d
```

### Using the data

```python
import qlib
from qlib.data import D

qlib.init(provider_uri="~/.qlib/qlib_data/crypto")
df = D.features(D.instruments(market="all"), ["$close", "$volume"], freq="day")
```


# qlib-crypto

A plugin-style extension layer for adapting qlib workflows to crypto markets.

Crypto assumptions in this plugin:
- 7*24 trading calendar
- no stock-style T+1 restriction
- no daily涨跌停 regime

## What is included

- spot OHLCV collector (Binance/OKX)
- perp metadata collector (funding/open-interest)
- calendar/instruments builders + dump pipeline
- crypto exchange constraints (qty step/min notional/tick/participation)
- crypto data handler/ops/strategy templates
- factor mining script and workflow YAML examples

## Quickstart

1. Collect + normalize:

```bash
python scripts/data_collector/crypto_ohlcv/collector.py download_data --source_dir ~/.qlib/crypto_ohlcv/source/1d --exchange binance --interval 1d
python scripts/data_collector/crypto_ohlcv/collector.py normalize_data --source_dir ~/.qlib/crypto_ohlcv/source/1d --normalize_dir ~/.qlib/crypto_ohlcv/normalize/1d --interval 1d
```

2. Build qlib data:

```bash
python -m qlib_crypto.data.pipeline --normalize_dir ~/.qlib/crypto_ohlcv/normalize/1d --qlib_dir ~/.qlib/crypto_data --exchange binance --freq day
```

3. Run workflow:

```bash
qrun qlib_crypto/examples/workflow_config_lightgbm_alpha158_crypto.yaml
qrun qlib_crypto/examples/crypto_spot_1min.yaml
```

## More docs

- `FACTOR_MINING.md`
- `MIGRATION.md`
- `ROADMAP.md`
- `TASKS.md`
- `RELEASE_CHECKLIST.md`
- `RELEASE_SIGNOFF.md`


## Release preflight

```bash
python -m qlib_crypto.release_preflight
# include dataset healthcheck:
python -m qlib_crypto.release_preflight --qlib_dir=~/.qlib/crypto_data
# include workflow execution gates:
python -m qlib_crypto.release_preflight --run_workflows=True --qlib_dir=~/.qlib/crypto_data
# in production runtime image (strict integration runtime gate):
python -m qlib_crypto.release_preflight --strict_runtime=True --qlib_dir=~/.qlib/crypto_data --run_workflows=True
```


## Release evidence report

```bash
python -m qlib_crypto.release_evidence --output=release_evidence.md --qlib_dir=~/.qlib/crypto_data --run_workflows=True
# production strict mode:
python -m qlib_crypto.release_evidence --output=release_evidence.md --qlib_dir=~/.qlib/crypto_data --run_workflows=True --strict_runtime=True
```

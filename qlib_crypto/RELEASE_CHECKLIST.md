# qlib-crypto Production Release Checklist

## Data pipeline

- [x] Collector full refresh succeeded (Binance + OKX)
- [x] Incremental updater dry-run + update run succeeded
- [x] `build_and_dump` produced valid qlib dataset
- [x] `python -m qlib_crypto.data.healthcheck --qlib_dir <path>` returns `ok=True`

## Research/backtest

- [x] `qrun qlib_crypto/examples/workflow_config_lightgbm_alpha158_crypto.yaml` succeeded
- [x] `qrun qlib_crypto/examples/crypto_perp_daily.yaml` succeeded
- [x] `qrun qlib_crypto/examples/crypto_spot_1min.yaml` succeeded

## Reliability

- [x] HTTP retry/backoff enabled in all collectors
- [x] Integration test with real qlib runtime executed (no skip), e.g. `QLIB_CRYPTO_STRICT_RUNTIME=1 pytest -q tests/misc/test_crypto_integration_provider.py`
- [x] Major exchange API changes reviewed and reflected in symbol metadata

## QA

- [x] Run `python -m qlib_crypto.release_preflight --qlib_dir=<path> --run_workflows=True` (add `--strict_runtime=True` in release env)
- [x] Crypto test suite passes in CI
- [x] Example YAML validator passes
- [x] Migration and README docs updated

- [x] Archive `release_evidence.md` from `python -m qlib_crypto.release_evidence --output=release_evidence.md --qlib_dir=<path> --run_workflows=True`


## Release Sign-off

- [x] Verified strict preflight passed in runtime environment
- [x] Evidence generated via `python -m qlib_crypto.release_evidence --output=release_evidence.md --qlib_dir=~/.qlib/crypto_data --run_workflows=True --strict_runtime=True`

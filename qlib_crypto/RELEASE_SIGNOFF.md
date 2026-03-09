# qlib-crypto Release Sign-off

Status: **READY**

## Environment

- provider (day): `~/.qlib/crypto_data`
- provider (1min): `~/.qlib/crypto_data_1min`

## Gates Executed

- `python -m qlib_crypto.release_preflight --strict_runtime=True --qlib_dir=~/.qlib/crypto_data --run_workflows=True`
  - result: pass
- `python -m qlib_crypto.release_evidence --output=release_evidence.md --qlib_dir=~/.qlib/crypto_data --run_workflows=True --strict_runtime=True`
  - result: pass

## Notes

- strict runtime integration test passed (`QLIB_CRYPTO_STRICT_RUNTIME=1`).
- all workflow gates in preflight passed (daily spot/perp and 1min).

# qlib-crypto Development Roadmap

## Vision
Build complete crypto support on top of qlib for:

- factor research
- feature engineering
- backtesting/simulation
- spot + perpetual market adaptation

## Phase Plan

### Phase 1: Data contract & ingestion (done/in progress)

- [x] Spot OHLCV contract and collector skeleton
- [x] Binance + OKX spot collection
- [x] 24/7 calendar builder
- [x] Instruments `all.txt` builder
- [x] Funding/OI/mark/index/basis collectors (stage-2 meta collector)
- [x] dump_bin end-to-end script and validation (pipeline script + env-guarded test)

### Phase 2: Crypto backtest exchange (current)

- [x] `REG_CRYPTO` region defaults
- [x] `CryptoExchange` class scaffold
- [x] Spot constraints: min qty, quantity step, min notional, long-only
- [x] Crypto fee configuration (maker/taker/funding placeholders)
- [x] Exchange-level tick-size rounding
- [x] Volume participation model by exchange + symbol

### Phase 3: Data handlers and alpha features

- [x] `Alpha158Crypto` handler baseline
- [x] crypto custom ops (`FundingZScore`, `BasisPct`, `OIRatio`)
- [x] strategy template for signal-driven crypto trading
- [x] perp-specific label templates and neutralization processors

### Phase 4: End-to-end examples

- [x] Spot daily backtest YAML (train + backtest)
- [x] Spot 1min realtime-style workflow template
- [x] Perp daily backtest YAML (with funding)
- [x] factor mining notebook/script + feature engineering recipe

### Phase 5: production hardening

- [x] realtime/incremental collector mode
- [x] metadata persistence for symbol rule changes
- [x] network resilience (retry/backoff in collectors)
- [x] CI tests with synthetic qlib crypto data fixture (env-guarded integration test)
- [x] docs and migration guide
- [x] release governance (checklist + dedicated CI workflow)

## Acceptance Criteria

1. `D.features()` can read OHLCV + crypto extension fields from qlib bin.
2. `Alpha158Crypto` dataset can train baseline models without manual patching.
3. `CryptoExchange` can run spot backtest with realistic order constraints.
4. end-to-end demo config runs with qrun.

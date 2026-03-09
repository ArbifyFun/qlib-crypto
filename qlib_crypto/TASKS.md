# qlib-crypto Task Board

## Immediate Tasks

- [x] T1: Add exchange multiplexer for Binance/OKX in collector
- [x] T2: Add pagination for long history retrieval
- [x] T3: Add calendar/instruments file builder from normalized CSV
- [x] T4: Add schema guards for crypto fields
- [x] T5: Implement spot `CryptoExchange` rules (long-only, min qty/step/notional)
- [x] T6: Implement `Alpha158Crypto` handler
- [x] T7: Implement custom crypto ops and strategy template
- [x] T8: Add perp metadata collector and unify symbol registry
- [x] T9: Add complete qrun config for factor training + backtest
- [x] T10: Add integration test with temporary qlib provider (env-guarded)
- [x] T11: Add build-and-dump pipeline script
- [x] T12: Add factor mining notebook/script and documentation
- [x] T13: Add incremental collector updater and metadata store
- [x] T14: Add migration and quickstart documentation
- [x] T15: Add qlib dataset healthcheck utility
- [x] T16: Add 7*24 1min workflow template and defaults
- [x] T17: Add retry/backoff HTTP resilience for collectors
- [x] T18: Add release checklist and dedicated crypto CI workflow

## Risks

- Exchange APIs vary in symbol format and field availability.
- Perp semantics (funding/mark/index/liquidation) need dedicated simulator constraints.
- Minute-level calendar and data volume may require optimized storage strategy.

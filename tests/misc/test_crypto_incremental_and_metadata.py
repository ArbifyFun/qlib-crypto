from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.collector.incremental import IncrementalUpdater, _to_exchange_symbol
from qlib_crypto.data.metadata_store import MetadataStore


def test_metadata_store_roundtrip(tmp_path: Path):
    store = MetadataStore(str(tmp_path / "meta.json"))
    store.upsert("BINANCE_BTCUSDT", {"qty_step": 0.001, "min_notional": 5})
    data = store.load()
    assert "BINANCE_BTCUSDT" in data
    assert data["BINANCE_BTCUSDT"]["qty_step"] == 0.001


def test_symbol_to_exchange_symbol():
    assert _to_exchange_symbol("binance", "BINANCE_BTCUSDT") == "BTCUSDT"
    assert _to_exchange_symbol("okx", "OKX_BTC_USDT_SWAP") == "BTC-USDT-SWAP"


def test_incremental_update_merges_new_rows(monkeypatch, tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir(parents=True)
    fp = src / "binance_btcusdt.csv"
    pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["BINANCE_BTCUSDT"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [1],
            "money": [1],
            "factor": [1],
            "vwap": [1],
            "trade_count": [1],
            "exchange": ["BINANCE"],
        }
    ).to_csv(fp, index=False)

    updater = IncrementalUpdater(str(src), exchange="binance", interval="1d")

    class DummyCollector:
        def get_data(self, symbol, interval, start, end):
            return pd.DataFrame(
                {
                    "date": ["2024-01-02"],
                    "open": [2],
                    "high": [2],
                    "low": [2],
                    "close": [2],
                    "volume": [2],
                    "money": [4],
                    "factor": [1],
                    "vwap": [2],
                    "trade_count": [2],
                    "exchange": ["BINANCE"],
                    "symbol": [symbol],
                }
            )

    monkeypatch.setattr(updater, "_new_collector", lambda: DummyCollector())

    n = updater.update(end="2024-01-03")
    assert n == 1
    out = pd.read_csv(fp)
    assert len(out) == 2
    assert "2024-01-02" in out["date"].astype(str).tolist()


def test_incremental_update_default_end_handles_tz_mismatch(monkeypatch, tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir(parents=True)
    fp = src / "binance_btcusdt.csv"
    pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["BINANCE_BTCUSDT"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [1],
            "money": [1],
            "factor": [1],
            "vwap": [1],
            "trade_count": [1],
            "exchange": ["BINANCE"],
        }
    ).to_csv(fp, index=False)

    updater = IncrementalUpdater(str(src), exchange="binance", interval="1d")

    class DummyCollector:
        def get_data(self, symbol, interval, start, end):
            return pd.DataFrame(
                {
                    "date": ["2024-01-02"],
                    "open": [2],
                    "high": [2],
                    "low": [2],
                    "close": [2],
                    "volume": [2],
                    "money": [4],
                    "factor": [1],
                    "vwap": [2],
                    "trade_count": [2],
                    "exchange": ["BINANCE"],
                    "symbol": [symbol],
                }
            )

    monkeypatch.setattr(updater, "_new_collector", lambda: DummyCollector())

    n = updater.update()
    assert n == 1

def test_incremental_update_tz_aware_csv_dates(monkeypatch, tmp_path: Path):
    src = tmp_path / "source"
    src.mkdir(parents=True)
    fp = src / "binance_btcusdt.csv"
    pd.DataFrame(
        {
            "date": ["2024-01-01T00:00:00+00:00"],
            "symbol": ["BINANCE_BTCUSDT"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
            "volume": [1],
            "money": [1],
            "factor": [1],
            "vwap": [1],
            "trade_count": [1],
            "exchange": ["BINANCE"],
        }
    ).to_csv(fp, index=False)

    updater = IncrementalUpdater(str(src), exchange="binance", interval="1d")

    class DummyCollector:
        def get_data(self, symbol, interval, start, end):
            assert start.tz is None
            assert end.tz is None
            return pd.DataFrame(
                {
                    "date": ["2024-01-02"],
                    "open": [2],
                    "high": [2],
                    "low": [2],
                    "close": [2],
                    "volume": [2],
                    "money": [4],
                    "factor": [1],
                    "vwap": [2],
                    "trade_count": [2],
                    "exchange": ["BINANCE"],
                    "symbol": [symbol],
                }
            )

    monkeypatch.setattr(updater, "_new_collector", lambda: DummyCollector())

    n = updater.update(end="2024-01-03")
    assert n == 1


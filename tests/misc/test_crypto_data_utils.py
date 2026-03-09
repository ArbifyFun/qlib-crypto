from pathlib import Path

import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


from qlib_crypto.data.builder import build_qlib_files
from qlib_crypto.data.dump import SPOT_BASE_FIELDS, ensure_crypto_schema
from scripts.data_collector.crypto_ohlcv.collector import CryptoCollector


def test_ensure_crypto_schema_adds_missing_columns():
    source = pd.DataFrame({"date": ["2024-01-01"], "symbol": ["BINANCE_BTCUSDT"]})
    result = ensure_crypto_schema(source)
    assert list(result.columns) == SPOT_BASE_FIELDS
    assert result.loc[0, "symbol"] == "BINANCE_BTCUSDT"


def test_build_qlib_files(tmp_path: Path):
    src = tmp_path / "normalize"
    src.mkdir(parents=True)
    pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "open": [1, 2],
        }
    ).to_csv(src / "BINANCE_BTCUSDT.csv", index=False)

    cal_path = tmp_path / "cal" / "day.txt"
    ins_path = tmp_path / "ins" / "all.txt"

    build_qlib_files(
        source_dir=str(src),
        calendar_path=str(cal_path),
        instruments_path=str(ins_path),
        exchange="binance",
    )

    cal_lines = cal_path.read_text().strip().splitlines()
    ins_lines = ins_path.read_text().strip().splitlines()
    assert cal_lines == ["2024-01-01", "2024-01-02"]
    assert ins_lines == ["BINANCE_BTCUSDT\t2024-01-01\t2024-01-02"]


def test_binance_pagination_logic():
    class DummyCollector(CryptoCollector):
        def __init__(self):
            self.calls = 0
            super().__init__(
                save_dir="/tmp/crypto_test",
                interval="1d",
                exchange="binance",
                quote_asset="USDT",
                limit_nums=1,
            )

        def _get_binance_instruments(self):
            return ["BTCUSDT"]

        def _request_json(self, url, params=None):
            if "exchangeInfo" in url:
                return {"symbols": []}
            self.calls += 1
            if self.calls == 1:
                # first page full
                return [[1704067200000, "1", "1", "1", "1", "1", 1704153599000, "1", "1", "0", "0", "0"]] * 1000
            if self.calls == 2:
                # second page tail
                return [[1704153600000, "2", "2", "2", "2", "2", 1704239999000, "4", "2", "0", "0", "0"]]
            return []

    c = DummyCollector()
    frame = c.get_data("BTCUSDT", "1d", pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-10"))
    assert c.calls >= 2
    assert not frame.empty

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.collector.crypto_meta import CryptoMetaCollector
from qlib_crypto.data.dump import merge_spot_and_meta


def test_binance_funding_pagination_and_schema():
    class Dummy(CryptoMetaCollector):
        def __init__(self):
            self.calls = 0

        def _request_json(self, url, params=None):
            self.calls += 1
            if self.calls == 1:
                return [
                    {"fundingTime": 1704067200000, "fundingRate": "0.0001", "markPrice": "42000"},
                    {"fundingTime": 1704096000000, "fundingRate": "0.0002", "markPrice": "43000"},
                ]
            return []

    c = Dummy()
    df = c.collect_binance_funding("BTCUSDT", start_time="2024-01-01", end_time="2024-01-02", limit=2)
    assert c.calls >= 2
    assert set(c.META_COLUMNS).issubset(df.columns)
    assert df["funding_rate"].notna().all()
    assert df["symbol"].iloc[0] == "BINANCE_BTCUSDT"


def test_merge_spot_and_meta():
    spot = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "symbol": ["BINANCE_BTCUSDT", "BINANCE_BTCUSDT"],
            "open": [1, 2],
            "high": [1, 2],
            "low": [1, 2],
            "close": [1, 2],
            "volume": [1, 2],
            "money": [1, 2],
            "factor": [1, 1],
            "vwap": [1, 2],
            "trade_count": [10, 20],
            "exchange": ["BINANCE", "BINANCE"],
        }
    )
    meta = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["BINANCE_BTCUSDT"],
            "funding_rate": [0.0001],
            "open_interest": [1000],
            "mark_price": [42000],
            "index_price": [41990],
            "basis": [10],
            "next_funding_time": ["2024-01-01 08:00:00"],
            "exchange": ["BINANCE"],
        }
    )
    merged = merge_spot_and_meta(spot, meta)
    assert "funding_rate" in merged.columns
    assert merged.loc[0, "funding_rate"] == 0.0001


def test_merge_meta_frames_keeps_complementary_columns():
    c = CryptoMetaCollector()
    funding = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["BINANCE_BTCUSDT"],
            "exchange": ["BINANCE"],
            "funding_rate": [0.0001],
            "open_interest": [pd.NA],
            "mark_price": [42000.0],
            "index_price": [pd.NA],
            "basis": [pd.NA],
            "next_funding_time": ["2024-01-01 08:00:00"],
        }
    )
    open_interest = pd.DataFrame(
        {
            "date": ["2024-01-01"],
            "symbol": ["BINANCE_BTCUSDT"],
            "exchange": ["BINANCE"],
            "funding_rate": [pd.NA],
            "open_interest": [1000.0],
            "mark_price": [pd.NA],
            "index_price": [pd.NA],
            "basis": [pd.NA],
            "next_funding_time": [pd.NA],
        }
    )

    merged = c.merge_meta_frames([funding, open_interest])

    assert len(merged) == 1
    assert merged.loc[0, "funding_rate"] == 0.0001
    assert merged.loc[0, "open_interest"] == 1000.0

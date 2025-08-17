import json
from pathlib import Path

import pandas as pd

from qlib.data.crypto import CryptoInstrumentProvider
from qlib.data.cache import H


def test_crypto_instrument_provider_json(tmp_path):
    H["i"].clear()
    data = [
        {"symbol": "BTC-USDT", "volume": 1200},
        {"symbol": "ETH-USDT", "volume": 50},
    ]
    fp = tmp_path / "pairs.json"
    fp.write_text(json.dumps(data))
    provider = CryptoInstrumentProvider(str(fp), min_volume=100)
    insts = provider.list_instruments({"market": "test", "filter_pipe": []}, as_list=True)
    assert insts == ["BTC-USDT"]
    # Call again to ensure cache path works
    insts2 = provider.list_instruments({"market": "test", "filter_pipe": []}, as_list=True)
    assert insts2 == ["BTC-USDT"]


def test_crypto_instrument_provider_csv(tmp_path):
    H["i"].clear()
    df = pd.DataFrame(
        [["BTC-USD", 100], ["ETH-USD", 200]], columns=["symbol", "volume"]
    )
    fp = tmp_path / "pairs.csv"
    df.to_csv(fp, index=False)
    provider = CryptoInstrumentProvider(str(fp), min_volume=150)
    insts = provider.list_instruments({"market": "csv", "filter_pipe": []}, as_list=True)
    assert insts == ["ETH-USD"]

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.contrib.data.processor import StablecoinNeutralize


def test_handler_supports_perp_label_mode_by_source_text():
    text = (ROOT / "qlib_crypto/contrib/data/handler.py").read_text()
    assert "label_mode=\"spot\"" in text
    assert "perp_funding_adj" in text
    assert "Ref($funding_rate" in text


def test_stablecoin_neutralize_multiindex():
    idx = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-01-01"), "BINANCE_BTCUSDT"),
            (pd.Timestamp("2024-01-01"), "BINANCE_BTCUSDC"),
            (pd.Timestamp("2024-01-01"), "OKX_ETH_USDT"),
        ],
        names=["datetime", "instrument"],
    )
    df = pd.DataFrame({"alpha": [1.0, 3.0, 10.0]}, index=idx)
    proc = StablecoinNeutralize()
    out = proc(df, ["alpha"])
    assert round(float(out.loc[(pd.Timestamp("2024-01-01"), "BINANCE_BTCUSDC"), "alpha"]), 6) == 0.0

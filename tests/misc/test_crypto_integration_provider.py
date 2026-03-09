from pathlib import Path
import os
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from scripts.dump_bin import DumpDataAll


def _import_runtime_or_skip():
    strict_runtime = os.environ.get("QLIB_CRYPTO_STRICT_RUNTIME", "0") == "1"
    # qlib runtime requires compiled cython ops in this repository layout.
    try:
        import qlib  # noqa: F401
        from qlib.data import D
        return D
    except Exception as e:  # pragma: no cover
        msg = f"qlib runtime unavailable in this env: {e}"
        if strict_runtime:
            pytest.fail(msg)
        pytest.skip(msg)


def test_temp_provider_features_roundtrip(tmp_path: Path):
    D = _import_runtime_or_skip()

    src = tmp_path / "src"
    src.mkdir(parents=True)
    pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "symbol": ["BINANCE_BTCUSDT", "BINANCE_BTCUSDT"],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.0, 2.0],
            "volume": [10.0, 20.0],
            "money": [10.0, 40.0],
            "factor": [1.0, 1.0],
            "vwap": [1.0, 2.0],
            "trade_count": [100, 200],
            "exchange": ["BINANCE", "BINANCE"],
        }
    ).to_csv(src / "BINANCE_BTCUSDT.csv", index=False)

    qlib_dir = tmp_path / "qlib_data"
    DumpDataAll(
        data_path=str(src),
        qlib_dir=str(qlib_dir),
        freq="day",
        max_workers=1,
        symbol_field_name="symbol",
        date_field_name="date",
        exclude_fields="symbol,exchange",
    ).dump()

    import qlib

    qlib.init(provider_uri=str(qlib_dir), region="crypto")
    feat = D.features(["BINANCE_BTCUSDT"], ["$close", "$volume"], start_time="2024-01-01", end_time="2024-01-02")
    assert not feat.empty

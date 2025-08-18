import pandas as pd
import pytest

from qlib.crypto import (
    CryptoCalendarProvider,
    CryptoInstrumentProvider,
    CryptoDatasetHandler,
)


@pytest.fixture(scope="module")
def sample_ohlcv():
    data = [
        ("2024-01-01", "BTC", 100, 110, 90, 105, 1000),
        ("2024-01-02", "BTC", 105, 115, 95, 110, 1100),
        ("2024-01-03", "BTC", 110, 120, 100, 115, 1200),
        ("2024-01-01", "ETH", 50, 55, 45, 52, 2000),
        ("2024-01-02", "ETH", 52, 58, 48, 56, 2100),
        ("2024-01-03", "ETH", 56, 60, 50, 58, 2200),
    ]
    columns = ["datetime", "symbol", "open", "high", "low", "close", "volume"]
    return pd.DataFrame(data, columns=columns)


def test_calendar_continuity(sample_ohlcv):
    cal_provider = CryptoCalendarProvider(sample_ohlcv)
    calendar = cal_provider.calendar()
    expected = pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"])
    assert list(calendar) == list(expected)
    # ensure consecutive days
    for prev, nxt in zip(calendar[:-1], calendar[1:]):
        assert (nxt - prev).days == 1


def test_instrument_filter(sample_ohlcv):
    inst_provider = CryptoInstrumentProvider(sample_ohlcv)
    filtered = inst_provider.list_instruments(["BTC", "LTC"], as_list=True)
    assert filtered == ["BTC"]


def test_feature_computation(sample_ohlcv):
    handler = CryptoDatasetHandler(sample_ohlcv)
    df = handler.fetch(instruments=["BTC"], fields=["return", "high_low_range"])
    assert pytest.approx(df.loc[(pd.Timestamp("2024-01-01"), "BTC"), "return"], rel=1e-6) == 0.05
    assert df.loc[(pd.Timestamp("2024-01-01"), "BTC"), "high_low_range"] == 20

import sys
import types

import pandas as pd
import pytest


# Provide stubs for optional cython extensions used by qlib
def _unimplemented(*args, **kwargs):  # pragma: no cover - simple stub
    raise NotImplementedError


rolling_stub = types.ModuleType("qlib.data._libs.rolling")
rolling_stub.rolling_slope = _unimplemented
rolling_stub.rolling_rsquare = _unimplemented
rolling_stub.rolling_resi = _unimplemented
sys.modules["qlib.data._libs.rolling"] = rolling_stub

expanding_stub = types.ModuleType("qlib.data._libs.expanding")
expanding_stub.expanding_slope = _unimplemented
expanding_stub.expanding_rsquare = _unimplemented
expanding_stub.expanding_resi = _unimplemented
sys.modules["qlib.data._libs.expanding"] = expanding_stub

from qlib.backtest.crypto_exchange import (
    CryptoExchange,
    PercentageFeeModel,
    LinearSlippageModel,
)
from qlib.backtest.decision import Order
from qlib.contrib.data.crypto_handler import CryptoHandler
from qlib.data.dataset.handler import DataHandlerLP
from qlib.config import C
from qlib.constant import REG_CRYPTO
from qlib.data.data import D


@pytest.fixture(scope="module")
def sample_ohlcv():
    data = [
        ("2024-01-01", "BTC", 100, 110, 90, 100, 1000),
        ("2024-01-02", "BTC", 100, 120, 95, 110, 1100),
        ("2024-01-03", "BTC", 110, 130, 105, 120, 1200),
    ]
    columns = ["datetime", "symbol", "open", "high", "low", "close", "volume"]
    df = pd.DataFrame(data, columns=columns)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index(["datetime", "symbol"], inplace=True)
    return df


def test_crypto_exchange_fee_slippage(sample_ohlcv):
    C.update(trade_unit=None, limit_threshold=None, deal_price="$close", region=REG_CRYPTO)

    def _features_stub(codes, fields, start_time=None, end_time=None, freq="day", disk_cache=True):
        idx = pd.MultiIndex.from_tuples([], names=["instrument", "datetime"])
        return pd.DataFrame(index=idx, columns=fields)

    D.features = _features_stub

    quote = sample_ohlcv[["close", "volume"]].copy()
    quote.rename(columns={"close": "$close", "volume": "$volume"}, inplace=True)
    quote["$factor"] = 1
    quote = quote.swaplevel().sort_index()
    quote.index.set_names(["instrument", "datetime"], inplace=True)

    exchange = CryptoExchange(
        freq="day",
        start_time="2024-01-01",
        end_time="2024-01-03",
        codes=["BTC"],
        fee_model=PercentageFeeModel(rate=0.001),
        slippage_model=LinearSlippageModel(rate=0.01),
        extra_quote=quote,
    )

    order = Order(
        stock_id="BTC",
        amount=1,
        direction=Order.BUY,
        start_time=pd.Timestamp("2024-01-03"),
        end_time=pd.Timestamp("2024-01-03"),
    )
    trade_val, trade_cost, trade_price = exchange.deal_order(order)
    assert trade_price == pytest.approx(120 * 1.01)
    assert trade_val == pytest.approx(120 * 1.01)
    assert trade_cost == pytest.approx(120 * 1.01 * 0.001)

    order = Order(
        stock_id="BTC",
        amount=1,
        direction=Order.SELL,
        start_time=pd.Timestamp("2024-01-03"),
        end_time=pd.Timestamp("2024-01-03"),
    )
    trade_val, trade_cost, trade_price = exchange.deal_order(order)
    assert trade_price == pytest.approx(120 * 0.99)
    assert trade_val == pytest.approx(120 * 0.99)
    assert trade_cost == pytest.approx(120 * 0.99 * 0.001)


def test_crypto_handler_features_labels(sample_ohlcv):
    handler_proto = CryptoHandler.__new__(CryptoHandler)
    _feat_expr, feat_names = handler_proto.get_feature_config()
    _label_expr, label_names = handler_proto.get_label_config()
    assert feat_names == ["RET", "VOLUME"]
    assert label_names == ["LABEL0"]

    df = sample_ohlcv.copy()
    df["RET"] = df.groupby(level="symbol")["close"].pct_change()
    df["VOLUME"] = df["volume"]
    df["LABEL0"] = df.groupby(level="symbol")["close"].shift(2) / df.groupby(level="symbol")["close"].shift(1) - 1

    features_df = df[["RET", "VOLUME"]].sort_index()
    labels_df = df[["LABEL0"]].sort_index()

    data_loader = {
        "class": "qlib.data.dataset.loader.StaticDataLoader",
        "kwargs": {"config": {"feature": features_df, "label": labels_df}},
    }

    handler = DataHandlerLP(
        instruments=["BTC"],
        start_time="2024-01-01",
        end_time="2024-01-03",
        data_loader=data_loader,
        infer_processors=[],
        learn_processors=[],
    )

    feats = handler.fetch(col_set="feature", data_key=DataHandlerLP.DK_L)
    labs = handler.fetch(col_set="label", data_key=DataHandlerLP.DK_L)
    idx = (pd.Timestamp("2024-01-03"), "BTC")
    assert feats.loc[idx, "RET"] == pytest.approx(120 / 110 - 1)
    assert feats.loc[idx, "VOLUME"] == 1200
    assert labs.loc[idx, "LABEL0"] == pytest.approx(100 / 110 - 1)

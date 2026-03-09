from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.backtest.cost import CryptoFeeConfig, calc_spot_trade_fee


def test_fee_model():
    cfg = CryptoFeeConfig(maker_fee=0.0002, taker_fee=0.0005, min_fee=0.1)
    assert calc_spot_trade_fee(1000, is_taker=True, fee_cfg=cfg) == 0.5
    assert calc_spot_trade_fee(10, is_taker=False, fee_cfg=cfg) == 0.1


def test_core_crypto_modules_exist():
    exchange_text = (ROOT / "qlib_crypto/backtest/exchange.py").read_text()
    handler_text = (ROOT / "qlib_crypto/contrib/data/handler.py").read_text()
    ops_text = (ROOT / "qlib_crypto/contrib/ops/crypto_ops.py").read_text()

    assert "class CryptoExchange" in exchange_text
    assert "class Alpha158Crypto" in handler_text
    assert "class FundingZScore" in ops_text


def test_exchange_supports_tick_and_volume_ratio():
    exchange_text = (ROOT / "qlib_crypto/backtest/exchange.py").read_text()
    assert "price_tick" in exchange_text
    assert "max_volume_ratio" in exchange_text


def test_strategy_default_no_t1():
    text = (ROOT / "qlib_crypto/contrib/strategy/signal_strategy.py").read_text()
    assert "hold_thresh=0" in text

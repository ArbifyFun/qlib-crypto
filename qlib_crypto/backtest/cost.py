from dataclasses import dataclass


@dataclass
class CryptoFeeConfig:
    maker_fee: float = 0.0002
    taker_fee: float = 0.0005
    funding_fee: float = 0.0
    min_fee: float = 0.0


def calc_spot_trade_fee(trade_val: float, is_taker: bool = True, fee_cfg: CryptoFeeConfig = CryptoFeeConfig()) -> float:
    """Calculate spot fee by maker/taker ratio with minimal fee floor."""
    ratio = fee_cfg.taker_fee if is_taker else fee_cfg.maker_fee
    return max(trade_val * ratio, fee_cfg.min_fee)

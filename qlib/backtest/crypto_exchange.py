from __future__ import annotations

from collections import defaultdict
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd

from .exchange import Exchange
from .decision import Order, OrderDir


class PercentageFeeModel:
    """Simple percentage based fee model."""

    def __init__(self, rate: float = 0.0):
        self.rate = rate

    def get_fee(self, amount: float, price: float) -> float:
        return abs(amount) * price * self.rate


class LinearSlippageModel:
    """Apply a linear slippage on price based on trade direction."""

    def __init__(self, rate: float = 0.0):
        self.rate = rate

    def get_trade_price(self, price: float, direction: OrderDir) -> float:
        if direction == OrderDir.BUY:
            return price * (1 + self.rate)
        elif direction == OrderDir.SELL:
            return price * (1 - self.rate)
        else:
            return price


class CryptoExchange(Exchange):
    """Exchange for cryptocurrency trading.

    Cryptocurrency market trades continuously.  This exchange adds simple
    fee and slippage models for 24/7 markets.
    """

    def __init__(
        self,
        *args,
        fee_model: PercentageFeeModel | None = None,
        slippage_model: LinearSlippageModel | None = None,
        **kwargs,
    ) -> None:
        # fees and slippage are handled by models,
        # so set costs to zero in parent class
        kwargs.setdefault("open_cost", 0.0)
        kwargs.setdefault("close_cost", 0.0)
        kwargs.setdefault("min_cost", 0.0)
        kwargs.setdefault("impact_cost", 0.0)
        super().__init__(*args, **kwargs)
        self.fee_model = fee_model or PercentageFeeModel()
        self.slippage_model = slippage_model or LinearSlippageModel()

    @staticmethod
    def calendar(
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        freq: str,
    ) -> pd.DatetimeIndex:
        """Generate a continuous trading calendar."""
        return pd.date_range(start_time, end_time, freq=freq)

    def deal_order(
        self,
        order: Order,
        trade_account=None,
        position=None,
        dealt_order_amount: Optional[Dict[str, float]] = None,
    ) -> Tuple[float, float, float]:
        dealt_order_amount = dealt_order_amount or defaultdict(float)
        if not self.check_order(order):
            order.deal_amount = 0.0
            return 0.0, 0.0, np.nan

        if trade_account is not None and position is not None:
            raise ValueError("trade_account and position can only choose one")

        pos = trade_account.current_position if trade_account else position
        trade_price, _, _ = super()._calc_trade_info_by_order(
            order,
            pos,
            dealt_order_amount,
        )
        trade_price = self.slippage_model.get_trade_price(
            trade_price,
            order.direction,
        )
        trade_val = order.deal_amount * trade_price
        trade_cost = self.fee_model.get_fee(order.deal_amount, trade_price)

        if trade_account:
            trade_account.update_order(
                order=order,
                trade_val=trade_val,
                cost=trade_cost,
                trade_price=trade_price,
            )
        elif position:
            position.update_order(
                order=order,
                trade_val=trade_val,
                cost=trade_cost,
                trade_price=trade_price,
            )

        return trade_val, trade_cost, trade_price

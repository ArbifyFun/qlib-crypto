from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np

from qlib.backtest.decision import Order
from qlib.backtest.exchange import Exchange


@dataclass
class SpotTradingRule:
    min_qty: float = 0.0
    qty_step: Optional[float] = None
    min_notional: float = 0.0
    long_only: bool = True
    price_tick: Optional[float] = None
    max_volume_ratio: Optional[float] = None


class CryptoExchange(Exchange):
    """Crypto spot exchange adaptation.

    Added constraints compared with stock-style exchange:
    - optional long-only enforcement
    - quantity step and minimum quantity
    - minimum notional check
    - optional price tick snapping
    - optional participation-rate limit by bar volume
    """

    def __init__(
        self,
        *args,
        min_qty: float = 0.0,
        qty_step: float | None = None,
        min_notional: float = 0.0,
        long_only: bool = True,
        price_tick: float | None = None,
        max_volume_ratio: float | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.rule = SpotTradingRule(
            min_qty=min_qty,
            qty_step=qty_step,
            min_notional=min_notional,
            long_only=long_only,
            price_tick=price_tick,
            max_volume_ratio=max_volume_ratio,
        )

    def round_amount_by_trade_unit(self, deal_amount: float, *args, **kwargs) -> float:
        """Use qty_step instead of stock trade_unit if provided."""
        if self.rule.qty_step is not None and self.rule.qty_step > 0:
            return np.floor(deal_amount / self.rule.qty_step) * self.rule.qty_step
        return super().round_amount_by_trade_unit(deal_amount, *args, **kwargs)

    def get_deal_price(self, stock_id, start_time, end_time, direction=None):
        price = super().get_deal_price(stock_id, start_time, end_time, direction=direction)
        if self.rule.price_tick is not None and self.rule.price_tick > 0 and price is not None and not np.isnan(price):
            return float(np.round(float(price) / self.rule.price_tick) * self.rule.price_tick)
        return price

    def _check_volume_ratio(self, order: Order) -> bool:
        if self.rule.max_volume_ratio is None:
            return True
        if self.rule.max_volume_ratio <= 0:
            return False
        try:
            bar_vol = self.get_volume(order.stock_id, order.start_time, order.end_time)
        except Exception:
            return False
        if bar_vol is None or np.isnan(bar_vol):
            return False
        return order.amount <= float(bar_vol) * self.rule.max_volume_ratio

    def check_order(self, order: Order) -> bool:
        if not super().check_order(order):
            return False

        if order.amount < self.rule.min_qty:
            return False

        if not self._check_volume_ratio(order):
            return False

        try:
            price = self.get_deal_price(order.stock_id, order.start_time, order.end_time, direction=order.direction)
        except Exception:
            return False

        if price is None or np.isnan(price):
            return False
        if order.amount * float(price) < self.rule.min_notional:
            return False
        return True

    def deal_order(self, order: Order, trade_account=None, position=None, dealt_order_amount=None):
        """Enforce long_only at execution stage: forbid naked short sells."""
        if dealt_order_amount is None:
            dealt_order_amount = defaultdict(float)
        if self.rule.long_only and order.direction == Order.SELL:
            pos = trade_account.current_position if trade_account is not None else position
            if pos is None:
                return 0.0, 0.0, np.nan
            amount = pos.get_stock_amount(order.stock_id) if pos.check_stock(order.stock_id) else 0.0
            if amount <= 0:
                order.deal_amount = 0.0
                return 0.0, 0.0, np.nan
        return super().deal_order(order, trade_account=trade_account, position=position, dealt_order_amount=dealt_order_amount)

from qlib.contrib.strategy.signal_strategy import TopkDropoutStrategy


class CryptoTopkStrategy(TopkDropoutStrategy):
    """Topk strategy defaults tuned for 7*24 crypto trading.

    - default `hold_thresh=0` to avoid stock-style T+1 behavior
    - default `forbid_all_trade_at_limit=False` because crypto has no daily涨跌停机制
    """

    def __init__(
        self,
        *,
        topk=30,
        n_drop=5,
        hold_thresh=0,
        only_tradable=True,
        forbid_all_trade_at_limit=False,
        **kwargs,
    ):
        super().__init__(
            topk=topk,
            n_drop=n_drop,
            hold_thresh=hold_thresh,
            only_tradable=only_tradable,
            forbid_all_trade_at_limit=forbid_all_trade_at_limit,
            **kwargs,
        )

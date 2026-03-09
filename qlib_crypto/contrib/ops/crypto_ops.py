import pandas as pd

from qlib.data.ops import ElemOperator, PairOperator


class FundingZScore(ElemOperator):
    def __init__(self, feature, window=20):
        self.window = window
        super().__init__(feature)

    def _load_internal(self, instrument, start_index, end_index, *args):
        series = self.feature.load(instrument, start_index, end_index, *args)
        mean = series.rolling(self.window, min_periods=1).mean()
        std = series.rolling(self.window, min_periods=1).std().replace(0, pd.NA)
        return (series - mean) / std


class BasisPct(PairOperator):
    """basis / close"""

    def _load_internal(self, instrument, start_index, end_index, *args):
        basis = self.feature_left.load(instrument, start_index, end_index, *args)
        close = self.feature_right.load(instrument, start_index, end_index, *args)
        return basis / close.replace(0, pd.NA)


class OIRatio(ElemOperator):
    def __init__(self, feature, window=5):
        self.window = window
        super().__init__(feature)

    def _load_internal(self, instrument, start_index, end_index, *args):
        series = self.feature.load(instrument, start_index, end_index, *args)
        return series / series.rolling(self.window, min_periods=1).mean().replace(0, pd.NA)

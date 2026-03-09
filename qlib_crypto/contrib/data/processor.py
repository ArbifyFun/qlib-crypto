from __future__ import annotations

import pandas as pd


class StablecoinNeutralize:
    """Neutralize feature columns by quote stablecoin groups.

    Expected index: MultiIndex with levels [datetime, instrument] or [instrument, datetime].
    Group id is inferred from symbol suffix (`USDT`, `USDC`, `FDUSD`, `DAI`).
    """

    STABLE_QUOTES = ("USDT", "USDC", "FDUSD", "DAI")

    @staticmethod
    def infer_quote(instrument: str) -> str:
        norm = instrument.replace("_", "")
        for q in StablecoinNeutralize.STABLE_QUOTES:
            if norm.endswith(q):
                return q
        return "OTHER"

    def __call__(self, df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
        if df.empty:
            return df
        out = df.copy()

        if isinstance(out.index, pd.MultiIndex):
            levels = list(out.index.names)
            if "instrument" in levels:
                inst_idx = levels.index("instrument")
            else:
                inst_idx = 1
            instruments = out.index.get_level_values(inst_idx).astype(str)
            group = instruments.map(self.infer_quote)
            level_dt = 0 if inst_idx == 1 else 1
            grouped = out.groupby([out.index.get_level_values(level_dt), group], sort=False)
            for col in feature_cols:
                if col in out.columns:
                    out[col] = out[col] - grouped[col].transform("mean")
            return out

        if "instrument" in out.columns and "datetime" in out.columns:
            group = out["instrument"].astype(str).map(self.infer_quote)
            grouped = out.groupby(["datetime", group], sort=False)
            for col in feature_cols:
                if col in out.columns:
                    out[col] = out[col] - grouped[col].transform("mean")
        return out

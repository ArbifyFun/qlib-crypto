from typing import Iterable

import pandas as pd


SPOT_BASE_FIELDS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "money",
    "factor",
    "vwap",
    "trade_count",
    "exchange",
]


PERP_EXTRA_FIELDS = [
    "funding_rate",
    "open_interest",
    "mark_price",
    "index_price",
    "basis",
    "taker_buy_base",
    "taker_buy_quote",
    "next_funding_time",
]


def ensure_crypto_schema(df: pd.DataFrame, required_fields: Iterable[str] = SPOT_BASE_FIELDS) -> pd.DataFrame:
    """Ensure dataframe contains required fields in fixed order."""
    frame = df.copy()
    for col in required_fields:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[list(required_fields)]


def merge_spot_and_meta(spot_df: pd.DataFrame, meta_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join spot bars with metadata on [date, symbol]."""
    spot = ensure_crypto_schema(spot_df, SPOT_BASE_FIELDS)
    if meta_df is None or meta_df.empty:
        for col in PERP_EXTRA_FIELDS:
            if col not in spot.columns:
                spot[col] = pd.NA
        return spot
    merged = spot.merge(meta_df, on=["date", "symbol"], how="left", suffixes=("", "_meta"))
    for col in PERP_EXTRA_FIELDS:
        if col not in merged.columns:
            merged[col] = pd.NA
    return merged

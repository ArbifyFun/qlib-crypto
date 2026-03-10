from __future__ import annotations

from pathlib import Path
from typing import Optional

import fire
import pandas as pd

from scripts.data_collector.crypto_ohlcv.collector import CryptoCollector


def _to_exchange_symbol(exchange: str, qlib_symbol: str) -> str:
    norm = qlib_symbol.upper()
    prefix = f"{exchange.upper()}_"
    if norm.startswith(prefix):
        body = norm[len(prefix) :]
    else:
        body = norm
    if exchange.lower() == "okx":
        return body.replace("_", "-")
    return body.replace("_", "")


class IncrementalUpdater:
    """Incrementally update collected source csv files by latest local date per symbol."""

    def __init__(self, source_dir: str, exchange: str = "binance", interval: str = "1d", quote_asset: str = "USDT"):
        self.source_dir = Path(source_dir).expanduser().resolve()
        self.source_dir.mkdir(parents=True, exist_ok=True)
        self.exchange = exchange
        self.interval = interval
        self.quote_asset = quote_asset

    def _new_collector(self) -> CryptoCollector:
        return CryptoCollector(
            save_dir=str(self.source_dir),
            interval=self.interval,
            exchange=self.exchange,
            quote_asset=self.quote_asset,
            max_workers=1,
        )

    def _infer_start(self, df: pd.DataFrame) -> pd.Timestamp:
        date_series = pd.to_datetime(df["date"], utc=True)
        last_dt = date_series.max().tz_localize(None)
        step = pd.Timedelta(days=1) if self.interval == "1d" else pd.Timedelta(minutes=1)
        return pd.Timestamp(last_dt) + step

    @staticmethod
    def _normalize_ts(ts: pd.Timestamp) -> pd.Timestamp:
        ts = pd.Timestamp(ts)
        if ts.tz is not None:
            return ts.tz_convert("UTC").tz_localize(None)
        return ts

    def update(self, end: Optional[str] = None, limit_nums: Optional[int] = None):
        files = sorted(self.source_dir.glob("*.csv"))
        if limit_nums is not None:
            files = files[: int(limit_nums)]
        if not files:
            return 0

        collector = self._new_collector()
        end_ts = self._normalize_ts(pd.Timestamp(end) if end is not None else pd.Timestamp.utcnow())
        updated = 0

        for fp in files:
            old_df = pd.read_csv(fp)
            if old_df.empty or "date" not in old_df.columns:
                continue
            qlib_symbol = str(old_df["symbol"].iloc[0])
            raw_symbol = _to_exchange_symbol(self.exchange, qlib_symbol)
            start_ts = self._infer_start(old_df)
            if start_ts >= end_ts:
                continue
            new_df = collector.get_data(raw_symbol, self.interval, start_ts, end_ts)
            if new_df is None or new_df.empty:
                continue

            # normalize saved symbol namespace to keep one file one symbol
            new_df = new_df.copy()
            new_df["symbol"] = qlib_symbol
            merged = pd.concat([old_df, new_df], sort=False, ignore_index=True)
            merged["date"] = pd.to_datetime(merged["date"], utc=True, format="mixed").dt.tz_localize(None)
            merged = merged.drop_duplicates(subset=["date"], keep="last").sort_values("date")
            merged.to_csv(fp, index=False)
            updated += 1
        return updated


if __name__ == "__main__":
    fire.Fire(IncrementalUpdater)

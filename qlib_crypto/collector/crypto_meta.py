from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from qlib_crypto.collector.http import build_retry_session
from qlib_crypto.data.symbol_registry import SymbolRegistry


@dataclass
class MetaRecord:
    date: pd.Timestamp
    symbol: str
    exchange: str
    funding_rate: float | None = None
    open_interest: float | None = None
    mark_price: float | None = None
    index_price: float | None = None
    basis: float | None = None
    next_funding_time: pd.Timestamp | None = None


class CryptoMetaCollector:
    """Collect perp metadata for funding/open-interest from Binance and OKX."""

    def __init__(self):
        self._session = build_retry_session()

    BINANCE_FUNDING_URL = "https://fapi.binance.com/fapi/v1/fundingRate"
    BINANCE_OI_HIST_URL = "https://fapi.binance.com/futures/data/openInterestHist"
    OKX_FUNDING_URL = "https://www.okx.com/api/v5/public/funding-rate-history"
    OKX_OI_URL = "https://www.okx.com/api/v5/public/open-interest"

    META_COLUMNS = [
        "date",
        "symbol",
        "exchange",
        "funding_rate",
        "open_interest",
        "mark_price",
        "index_price",
        "basis",
        "next_funding_time",
    ]

    def _request_json(self, url: str, params: Optional[Dict] = None):
        resp = self._session.get(url, params=params, timeout=(5, 30))
        resp.raise_for_status()
        payload = resp.json()
        if isinstance(payload, dict) and payload.get("code") not in (None, "0"):
            raise ValueError(f"request failed: {payload}")
        return payload

    @staticmethod
    def _ensure_ts(v) -> pd.Timestamp:
        ts = pd.to_datetime(v, utc=True)
        return pd.Timestamp(ts).tz_convert(None)

    def collect_binance_funding(self, symbol: str, start_time=None, end_time=None, limit: int = 1000, qlib_symbol: bool = True) -> pd.DataFrame:
        start = pd.Timestamp(start_time, tz="UTC") if start_time is not None else None
        end = pd.Timestamp(end_time, tz="UTC") if end_time is not None else None
        rows: List[dict] = []
        cursor_ms = int(start.timestamp() * 1000) if start is not None else None

        while True:
            params = {"symbol": symbol, "limit": limit}
            if cursor_ms is not None:
                params["startTime"] = cursor_ms
            if end is not None:
                params["endTime"] = int(end.timestamp() * 1000)
            chunk = self._request_json(self.BINANCE_FUNDING_URL, params=params)
            if not chunk:
                break
            rows.extend(chunk)
            if len(chunk) < limit:
                break
            next_ms = int(chunk[-1]["fundingTime"]) + 1
            if cursor_ms is not None and next_ms <= cursor_ms:
                break
            cursor_ms = next_ms

        if not rows:
            return pd.DataFrame(columns=self.META_COLUMNS)

        frame = pd.DataFrame(rows)
        frame["date"] = pd.to_datetime(frame["fundingTime"], unit="ms", utc=True).dt.tz_convert(None)
        frame["symbol"] = SymbolRegistry.to_qlib_symbol("BINANCE", symbol) if qlib_symbol else symbol
        frame["exchange"] = "BINANCE"
        frame["funding_rate"] = pd.to_numeric(frame.get("fundingRate"), errors="coerce")
        frame["mark_price"] = pd.to_numeric(frame.get("markPrice"), errors="coerce")
        frame["open_interest"] = pd.NA
        frame["index_price"] = pd.NA
        frame["basis"] = pd.NA
        frame["next_funding_time"] = pd.NA
        return frame[self.META_COLUMNS].sort_values("date").drop_duplicates(["date"])

    def collect_binance_open_interest(self, symbol: str, period: str = "1d", limit: int = 500, qlib_symbol: bool = True) -> pd.DataFrame:
        payload = self._request_json(
            self.BINANCE_OI_HIST_URL,
            params={"symbol": symbol, "period": period, "limit": limit},
        )
        if not payload:
            return pd.DataFrame(columns=self.META_COLUMNS)
        frame = pd.DataFrame(payload)
        frame["date"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
        frame["symbol"] = SymbolRegistry.to_qlib_symbol("BINANCE", symbol) if qlib_symbol else symbol
        frame["exchange"] = "BINANCE"
        frame["open_interest"] = pd.to_numeric(frame.get("sumOpenInterest"), errors="coerce")
        frame["funding_rate"] = pd.NA
        frame["mark_price"] = pd.NA
        frame["index_price"] = pd.NA
        frame["basis"] = pd.NA
        frame["next_funding_time"] = pd.NA
        return frame[self.META_COLUMNS].sort_values("date").drop_duplicates(["date"])

    def collect_okx_funding(self, inst_id: str, limit: int = 100, qlib_symbol: bool = True) -> pd.DataFrame:
        payload = self._request_json(self.OKX_FUNDING_URL, params={"instId": inst_id, "limit": str(limit)})
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not rows:
            return pd.DataFrame(columns=self.META_COLUMNS)

        frame = pd.DataFrame(rows)
        frame["date"] = pd.to_datetime(frame["fundingTime"], unit="ms", utc=True).dt.tz_convert(None)
        frame["symbol"] = SymbolRegistry.to_qlib_symbol("OKX", inst_id) if qlib_symbol else inst_id
        frame["exchange"] = "OKX"
        frame["funding_rate"] = pd.to_numeric(frame.get("fundingRate"), errors="coerce")
        frame["next_funding_time"] = pd.to_datetime(frame.get("nextFundingTime"), unit="ms", utc=True).dt.tz_convert(None)
        frame["mark_price"] = pd.NA
        frame["open_interest"] = pd.NA
        frame["index_price"] = pd.NA
        frame["basis"] = pd.NA
        return frame[self.META_COLUMNS].sort_values("date").drop_duplicates(["date"])

    def collect_okx_open_interest(self, inst_id: str, qlib_symbol: bool = True) -> pd.DataFrame:
        payload = self._request_json(self.OKX_OI_URL, params={"instId": inst_id})
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        if not rows:
            return pd.DataFrame(columns=self.META_COLUMNS)
        frame = pd.DataFrame(rows)
        # endpoint is snapshot-like: retain ts if provided else now
        if "ts" in frame.columns:
            frame["date"] = pd.to_datetime(frame["ts"], unit="ms", utc=True).dt.tz_convert(None)
        else:
            frame["date"] = pd.Timestamp.utcnow().tz_localize(None)
        frame["symbol"] = SymbolRegistry.to_qlib_symbol("OKX", inst_id) if qlib_symbol else inst_id
        frame["exchange"] = "OKX"
        frame["open_interest"] = pd.to_numeric(frame.get("oi"), errors="coerce")
        frame["funding_rate"] = pd.NA
        frame["mark_price"] = pd.NA
        frame["index_price"] = pd.NA
        frame["basis"] = pd.NA
        frame["next_funding_time"] = pd.NA
        return frame[self.META_COLUMNS].sort_values("date").drop_duplicates(["date"])

    def merge_meta_frames(self, frames: List[pd.DataFrame]) -> pd.DataFrame:
        valid = [f for f in frames if f is not None and not f.empty]
        if not valid:
            return pd.DataFrame(columns=self.META_COLUMNS)
        df = pd.concat(valid, ignore_index=True, sort=False)
        df = df.sort_values(["symbol", "date"]).drop_duplicates(["symbol", "date"], keep="last")
        return df[self.META_COLUMNS]

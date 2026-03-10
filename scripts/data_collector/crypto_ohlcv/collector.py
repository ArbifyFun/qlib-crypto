import abc
import sys
from abc import ABC
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import fire
import pandas as pd
from loguru import logger
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_retry_session(total=5, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504)):
    retry = Retry(
        total=total,
        connect=total,
        read=total,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "qlib-crypto-collector/1.0"})
    return session

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
from data_collector.base import BaseCollector, BaseNormalize, BaseRun


BINANCE_SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_SPOT_KLINES = "https://api.binance.com/api/v3/klines"

OKX_SPOT_INSTRUMENTS = "https://www.okx.com/api/v5/public/instruments"
OKX_HISTORY_CANDLES = "https://www.okx.com/api/v5/market/history-candles"


class CryptoCollector(BaseCollector):
    """Crypto OHLCV collector with pluggable exchange backends."""

    DATA_CONTRACT_COLUMNS = [
        "date",
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
        "symbol",
    ]

    SUPPORTED_EXCHANGES = {"binance", "okx"}

    def __init__(
        self,
        save_dir: [str, Path],
        start=None,
        end=None,
        interval="1d",
        max_workers=1,
        max_collector_count=2,
        delay=0,
        check_data_length: int = None,
        limit_nums: int = None,
        exchange: str = "binance",
        quote_asset: str = "USDT",
    ):
        self.exchange = exchange.lower()
        if self.exchange not in self.SUPPORTED_EXCHANGES:
            raise ValueError(f"unsupported exchange: {exchange}, supported={sorted(self.SUPPORTED_EXCHANGES)}")
        self.quote_asset = quote_asset.upper()
        self._session = build_retry_session()
        super().__init__(
            save_dir=save_dir,
            start=start,
            end=end,
            interval=interval,
            max_workers=max_workers,
            max_collector_count=max_collector_count,
            delay=delay,
            check_data_length=check_data_length,
            limit_nums=limit_nums,
        )

    def _request_json(self, url: str, params: Optional[Dict] = None):
        response = self._session.get(url, params=params, timeout=(5, 30))
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") not in (None, "0"):
            raise ValueError(f"request failed: {payload}")
        return payload

    def _map_interval(self, interval: str) -> Tuple[str, str, pd.Timedelta]:
        mapping = {
            self.INTERVAL_1d: ("1d", "1Dutc", pd.Timedelta(days=1)),
            self.INTERVAL_1min: ("1m", "1m", pd.Timedelta(minutes=1)),
        }
        if interval not in mapping:
            raise ValueError(f"unsupported interval: {interval}")
        return mapping[interval]

    @staticmethod
    def _to_utc_timestamp(dt_obj) -> pd.Timestamp:
        ts = pd.Timestamp(dt_obj)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts

    def _normalize_exchange_symbol(self, symbol: str) -> str:
        return symbol.replace("-", "_")

    @staticmethod
    def _safe_vwap(quote_volume: pd.Series, volume: pd.Series) -> pd.Series:
        vol = pd.to_numeric(volume, errors="coerce")
        quote = pd.to_numeric(quote_volume, errors="coerce")
        return quote.div(vol.where(vol != 0, np.nan))

    def get_instrument_list(self) -> List[str]:
        if self.exchange == "binance":
            return self._get_binance_instruments()
        return self._get_okx_instruments()

    def _get_binance_instruments(self) -> List[str]:
        payload = self._request_json(BINANCE_SPOT_EXCHANGE_INFO)
        symbols = []
        for row in payload.get("symbols", []):
            if row.get("status") != "TRADING":
                continue
            if row.get("quoteAsset") != self.quote_asset:
                continue
            if not row.get("isSpotTradingAllowed"):
                continue
            symbols.append(row["symbol"])
        logger.info(f"loaded {len(symbols)} Binance spot symbols with quote={self.quote_asset}")
        return sorted(symbols)

    def _get_okx_instruments(self) -> List[str]:
        payload = self._request_json(OKX_SPOT_INSTRUMENTS, params={"instType": "SPOT"})
        symbols = []
        for row in payload.get("data", []):
            if row.get("state") != "live":
                continue
            inst_id = row.get("instId", "")
            if not inst_id.endswith(f"-{self.quote_asset}"):
                continue
            symbols.append(inst_id)
        logger.info(f"loaded {len(symbols)} OKX spot symbols with quote={self.quote_asset}")
        return sorted(symbols)

    def normalize_symbol(self, symbol: str):
        return f"{self.exchange.upper()}_{self._normalize_exchange_symbol(symbol)}"

    def _to_contract_frame_binance(self, symbol: str, raw_data: list) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=self.DATA_CONTRACT_COLUMNS)

        df = pd.DataFrame(
            raw_data,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "close_time",
                "quote_volume",
                "trade_count",
                "taker_buy_base",
                "taker_buy_quote",
                "ignore",
            ],
        )
        numeric_columns = ["open", "high", "low", "close", "volume", "quote_volume", "trade_count"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None)
        df["money"] = df["quote_volume"]
        df["factor"] = 1.0
        df["vwap"] = self._safe_vwap(df["quote_volume"], df["volume"])
        df["exchange"] = "BINANCE"
        df["symbol"] = symbol

        return df[self.DATA_CONTRACT_COLUMNS].copy()

    def _to_contract_frame_okx(self, symbol: str, raw_data: list) -> pd.DataFrame:
        if not raw_data:
            return pd.DataFrame(columns=self.DATA_CONTRACT_COLUMNS)

        df = pd.DataFrame(
            raw_data,
            columns=[
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "vol_ccy",
                "quote_volume",
                "confirm",
            ],
        )

        numeric_columns = ["open", "high", "low", "close", "volume", "quote_volume"]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["date"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None)
        df["money"] = df["quote_volume"]
        df["factor"] = 1.0
        df["vwap"] = self._safe_vwap(df["quote_volume"], df["volume"])
        df["trade_count"] = pd.NA
        df["exchange"] = "OKX"
        df["symbol"] = symbol

        return df[self.DATA_CONTRACT_COLUMNS].copy()

    def _get_data_binance(self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp):
        interval_binance, _, step = self._map_interval(interval)
        start_ts = self._to_utc_timestamp(start_datetime)
        end_ts = self._to_utc_timestamp(end_datetime)
        current = start_ts
        rows = []

        while current < end_ts:
            params = {
                "symbol": symbol,
                "interval": interval_binance,
                "startTime": int(current.timestamp() * 1000),
                "endTime": int(end_ts.timestamp() * 1000),
                "limit": 1000,
            }
            chunk = self._request_json(BINANCE_SPOT_KLINES, params=params)
            if not chunk:
                break
            rows.extend(chunk)
            last_open = pd.to_datetime(chunk[-1][0], unit="ms", utc=True)
            next_open = last_open + step
            if next_open <= current:
                break
            current = next_open
            if len(chunk) < 1000:
                break

        frame = self._to_contract_frame_binance(symbol, rows)
        return frame.drop_duplicates(subset=["date"]).sort_values("date")

    def _get_data_okx(self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp):
        _, interval_okx, step = self._map_interval(interval)
        start_ts = self._to_utc_timestamp(start_datetime)
        end_ts = self._to_utc_timestamp(end_datetime)
        cursor = end_ts
        rows = []

        while cursor > start_ts:
            params = {
                "instId": symbol,
                "bar": interval_okx,
                "before": str(int(cursor.timestamp() * 1000)),
                "after": str(int(start_ts.timestamp() * 1000)),
                "limit": "300",
            }
            payload = self._request_json(OKX_HISTORY_CANDLES, params=params)
            chunk = payload.get("data", [])
            if not chunk:
                break
            rows.extend(chunk)
            oldest_open = pd.to_datetime(chunk[-1][0], unit="ms", utc=True)
            next_cursor = oldest_open - step
            if next_cursor >= cursor:
                break
            cursor = next_cursor
            if len(chunk) < 300:
                break

        frame = self._to_contract_frame_okx(symbol, rows)
        return frame.drop_duplicates(subset=["date"]).sort_values("date")

    def get_data(self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp) -> pd.DataFrame:
        if self.exchange == "binance":
            return self._get_data_binance(symbol, interval, start_datetime, end_datetime)
        return self._get_data_okx(symbol, interval, start_datetime, end_datetime)


class CryptoCollector1d(CryptoCollector, ABC):
    pass


class CryptoCollector1min(CryptoCollector, ABC):
    pass


class CryptoNormalize(BaseNormalize):
    @staticmethod
    def normalize_crypto(df: pd.DataFrame, calendar_list: list = None, date_field_name: str = "date"):
        if df.empty:
            return df

        df = df.copy()
        df[date_field_name] = pd.to_datetime(df[date_field_name])
        df = df.sort_values(date_field_name)
        df = df.drop_duplicates(subset=[date_field_name], keep="last")

        if calendar_list:
            cal = pd.DataFrame(index=pd.to_datetime(calendar_list))
            df = cal.join(df.set_index(date_field_name), how="left").reset_index(names=date_field_name)

        return df

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.normalize_crypto(df, self._calendar_list, self._date_field_name)


class CryptoNormalize1d(CryptoNormalize):
    def _get_calendar_list(self):
        return None


class CryptoNormalize1min(CryptoNormalize):
    def _get_calendar_list(self):
        return None


class Run(BaseRun):
    @property
    def collector_class_name(self):
        return f"CryptoCollector{self.interval}"

    @property
    def normalize_class_name(self):
        return f"CryptoNormalize{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR


if __name__ == "__main__":
    fire.Fire(Run)

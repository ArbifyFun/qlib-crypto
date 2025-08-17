import sys
from pathlib import Path

import os

import ccxt
import fire
import pandas as pd
from loguru import logger

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))
from data_collector.base import BaseCollector, BaseNormalize, BaseRun
from data_collector.utils import deco_retry
from qlib.data.storage.file_storage import (
    FileCalendarStorage,
    FileFeatureStorage,
    FileInstrumentStorage,
)


def dump_to_qlib(
    normalize_dir: Path,
    qlib_dir: Path,
    freq: str,
    date_field_name: str = "date",
    symbol_field_name: str = "symbol",
):
    """Convert normalized csv data to Qlib format."""

    normalize_dir = Path(normalize_dir).expanduser()
    qlib_dir = Path(qlib_dir).expanduser()

    # 创建 Qlib 所需的文件夹结构
    for sub in ["calendars", "features", "instruments"]:
        qlib_dir.joinpath(sub).mkdir(parents=True, exist_ok=True)

    provider_uri = {freq: str(qlib_dir)}

    file_list = sorted(normalize_dir.glob("*.csv"))
    calendar_set = set()
    inst_dict = {}
    data_map = {}
    for file_path in file_list:
        df = pd.read_csv(file_path, parse_dates=[date_field_name])
        symbol = file_path.stem
        # 收集日历和标的信息
        calendar_set.update(df[date_field_name])
        inst_dict[symbol] = [(df[date_field_name].min(), df[date_field_name].max())]
        data_map[symbol] = df

    calendar_list = sorted(calendar_set)
    fmt = "%Y-%m-%d" if freq == "1d" else "%Y-%m-%d %H:%M:%S"
    cs = FileCalendarStorage(freq=freq, future=False, provider_uri=provider_uri)
    cs.clear()
    cs.extend([pd.Timestamp(d).strftime(fmt) for d in calendar_list])

    # 写入交易所和股票信息
    is_storage = FileInstrumentStorage(market="all", freq=freq, provider_uri=provider_uri)
    is_storage.clear()
    is_storage.update(inst_dict)

    # 写入特征数据
    for symbol, df in data_map.items():
        for field in [c for c in df.columns if c not in [date_field_name, symbol_field_name]]:
            fs = FileFeatureStorage(symbol, field, freq, provider_uri=provider_uri)
            fs.clear()
            fs.write(df[field].astype(float).values, index=0)


class CryptoCollector(BaseCollector):
    """使用 ccxt 从交易所抓取 OHLCV 数据."""

    INTERVAL_1h = "1h"
    # 将 Qlib 的频率映射到 ccxt 的 `timeframe` 参数
    TIMEFRAME_MAP = {
        BaseCollector.INTERVAL_1min: "1m",
        INTERVAL_1h: "1h",
        BaseCollector.INTERVAL_1d: "1d",
    }

    DEFAULT_START_DATETIME_1H = pd.Timestamp("2017-01-01")
    DEFAULT_END_DATETIME_1H = BaseCollector.DEFAULT_END_DATETIME_1D

    def __init__(
        self,
        save_dir: [str, Path],
        symbols: list = None,
        start=None,
        end=None,
        interval="1d",
        exchange: str = "okx",
        max_workers=1,
        max_collector_count=2,
        delay=1,
        check_data_length: int = None,
        limit_nums: int = None,
    ):
        self._symbols = symbols or []
        self.exchange = exchange
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
        if self.interval not in self.TIMEFRAME_MAP:
            raise ValueError(f"interval error: {self.interval}")

        api_key = os.getenv("OKX_API_KEY")
        secret = os.getenv("OKX_API_SECRET")
        password = os.getenv("OKX_API_PASSPHRASE")
        exchange_cls = getattr(ccxt, self.exchange)
        self._client = exchange_cls(
            {
                "apiKey": api_key,
                "secret": secret,
                "password": password,
                "enableRateLimit": True,
            }
        )

    def get_instrument_list(self):
        return self._symbols

    def normalize_symbol(self, symbol):
        return symbol.replace("/", "_").replace("-", "_")

    @deco_retry
    def get_data_from_remote(self, symbol, interval, start, end):
        """通过 ccxt 的 `fetch_ohlcv` 接口获取数据"""
        error_msg = f"{symbol}-{interval}-{start}-{end}"
        timeframe = self.TIMEFRAME_MAP[interval]
        since = int(pd.Timestamp(start, tz="UTC").timestamp() * 1000)
        end_ts = int(pd.Timestamp(end, tz="UTC").timestamp() * 1000)
        all_data = []
        while since <= end_ts:
            try:
                ohlcvs = self._client.fetch_ohlcv(
                    symbol.replace("_", "/"), timeframe=timeframe, since=since, limit=1000
                )
            except Exception as e:  # pragma: no cover - 网络异常不计入覆盖率
                logger.warning(f"{error_msg}: {e}")
                break
            if not ohlcvs:
                break
            all_data.extend(ohlcvs)
            since = ohlcvs[-1][0] + self._client.parse_timeframe(timeframe) * 1000
            self.sleep()
        df = pd.DataFrame(all_data, columns=["ts", "open", "high", "low", "close", "volume"])
        df["date"] = pd.to_datetime(df["ts"], unit="ms")
        df = df[(df["date"] >= pd.Timestamp(start)) & (df["date"] <= pd.Timestamp(end))]
        df.sort_values("date", inplace=True)
        return df[["date", "open", "high", "low", "close", "volume"]]

    def get_data(
        self, symbol: str, interval: str, start_datetime: pd.Timestamp, end_datetime: pd.Timestamp
    ) -> pd.DataFrame:
        return self.get_data_from_remote(symbol, interval, start_datetime, end_datetime)


class CryptoNormalize(BaseNormalize):
    DAILY_FORMAT = "%Y-%m-%d"

    @staticmethod
    def normalize_crypto(
        df: pd.DataFrame,
        calendar_list: list = None,
        date_field_name: str = "date",
        symbol_field_name: str = "symbol",
    ):
        if df.empty:
            return df
        df = df.copy()
        # 将日期列设置为索引，并确保索引唯一有序
        df.set_index(date_field_name, inplace=True)
        df.index = pd.to_datetime(df.index)
        df = df[~df.index.duplicated(keep="first")]
        if calendar_list is not None:
            df = df.reindex(pd.DataFrame(index=calendar_list).loc[df.index.min() : df.index.max()].index)
        df.sort_index(inplace=True)
        df.index.names = [date_field_name]
        return df.reset_index()

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        return self.normalize_crypto(df, self._calendar_list, self._date_field_name, self._symbol_field_name)


class CryptoNormalize1d(CryptoNormalize):
    def _get_calendar_list(self):
        return None


class CryptoNormalize1h(CryptoNormalize1d):
    pass


class CryptoNormalize1min(CryptoNormalize1d):
    pass


class Run(BaseRun):
    def __init__(self, source_dir=None, normalize_dir=None, max_workers=1, interval="1d"):
        super().__init__(source_dir, normalize_dir, max_workers, interval)

    @property
    def collector_class_name(self):
        return f"CryptoCollector"

    @property
    def normalize_class_name(self):
        return f"CryptoNormalize{self.interval}"

    @property
    def default_base_dir(self) -> [Path, str]:
        return CUR_DIR

    def download_data(
        self,
        max_collector_count=2,
        delay=1,
        start=None,
        end=None,
        check_data_length: int = None,
        limit_nums=None,
        symbols: list = None,
    ):
        # 默认延迟 1 秒，配合 ccxt 的限流机制
        super(Run, self).download_data(
            max_collector_count,
            delay,
            start,
            end,
            check_data_length,
            limit_nums,
            symbols=symbols,
        )

    def normalize_data(self, date_field_name: str = "date", symbol_field_name: str = "symbol"):
        # 调用父类方法完成数据规范化
        super(Run, self).normalize_data(date_field_name, symbol_field_name)

    def dump_to_qlib(self, qlib_dir, date_field_name: str = "date", symbol_field_name: str = "symbol"):
        # 将规范化后的数据写入 Qlib 目录
        dump_to_qlib(self.normalize_dir, qlib_dir, self.interval, date_field_name, symbol_field_name)


if __name__ == "__main__":
    fire.Fire(Run)

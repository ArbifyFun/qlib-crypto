import pandas as pd
from typing import List, Union, Dict, Optional

from ..data.data import CalendarProvider, InstrumentProvider


class CryptoCalendarProvider(CalendarProvider):
    """A minimal calendar provider for crypto data based on an in-memory DataFrame."""

    def __init__(self, df: pd.DataFrame):
        self._calendar = sorted(pd.to_datetime(df["datetime"].unique()))

    def load_calendar(self, freq: str, future: bool):
        if freq != "day":
            raise ValueError("Only daily frequency supported in tests")
        return self._calendar


class CryptoInstrumentProvider(InstrumentProvider):
    """Instrument provider using in-memory symbols."""

    def __init__(self, df: pd.DataFrame):
        self._instruments = sorted(df["symbol"].unique())

    def list_instruments(self, instruments, start_time: Optional[str] = None, end_time: Optional[str] = None,
                         freq: str = "day", as_list: bool = False):
        inst_type = self.get_inst_type(instruments)
        if inst_type == self.LIST:
            base = list(instruments)
        elif inst_type == self.CONF:
            base = list(self._instruments)
        elif inst_type == self.DICT:
            base = list(instruments.keys())
        else:
            base = []
        base = [s for s in base if s in self._instruments]
        if as_list:
            return base
        return {s: (start_time, end_time) for s in base}


class CryptoDatasetHandler:
    """Simple dataset handler that computes basic features."""

    def __init__(self, df: pd.DataFrame):
        df = df.copy()
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index(["datetime", "symbol"], inplace=True)
        df.sort_index(inplace=True)
        df["return"] = df["close"] / df["open"] - 1
        df["high_low_range"] = df["high"] - df["low"]
        self.data = df

    def fetch(self, instruments: Optional[List[str]] = None, start: Optional[str] = None,
              end: Optional[str] = None, fields: Optional[List[str]] = None) -> pd.DataFrame:
        df = self.data
        if instruments is not None:
            df = df[df.index.get_level_values("symbol").isin(instruments)]
        if start or end:
            df = df.loc[(slice(pd.Timestamp(start) if start else None, pd.Timestamp(end) if end else None), slice(None)), :]
        if fields is not None:
            df = df[fields]
        return df.copy()

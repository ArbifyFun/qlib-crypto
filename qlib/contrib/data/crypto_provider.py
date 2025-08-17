import pandas as pd
from pathlib import Path
from typing import List, Dict

from qlib.config import C
from qlib.data.data import BaseProvider, Cal, Inst, ProviderBackendMixin
from qlib.utils import code_to_fname


class FileDatasetStorage:
    """Simple file-based storage for crypto OHLCV data."""

    def __init__(self, instrument: str, freq: str, provider_uri: Dict[str, str] = None, **kwargs):
        self.instrument = code_to_fname(instrument)
        self.freq = freq
        provider_uri = provider_uri or C.provider_uri
        uri = provider_uri.get(freq, next(iter(provider_uri.values())))
        self.file_path = Path(uri).expanduser().resolve() / f"{self.instrument}.csv"
        self._data = pd.read_csv(self.file_path, index_col=0, parse_dates=True)

    def read(self, fields: List[str], start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        df = self._data.loc[start_time:end_time, fields]
        df.index = pd.DatetimeIndex(df.index)
        return df


class CryptoProvider(BaseProvider, ProviderBackendMixin):
    """Feature provider for cryptocurrency OHLCV data."""

    def __init__(self, backend: Dict = None):
        self.backend = backend or {"class": "FileDatasetStorage", "module_path": "qlib.contrib.data.crypto_provider"}

    # Calendar and instrument utilities are delegated to existing providers
    def calendar(self, start_time=None, end_time=None, freq="day", future=False):
        return Cal.calendar(start_time, end_time, freq, future)

    def instruments(self, market="all", filter_pipe=None, start_time=None, end_time=None):
        return Inst.instruments(market, filter_pipe)

    def list_instruments(self, instruments, start_time=None, end_time=None, freq="day", as_list=False):
        return Inst.list_instruments(instruments, start_time, end_time, freq, as_list)

    def features(self, instruments, fields, start_time=None, end_time=None, freq="day", **kwargs):
        fields = [str(f).lstrip("$") for f in fields]
        inst_list = list(instruments) if isinstance(instruments, (list, tuple, set)) else [instruments]
        cal = self.calendar(start_time, end_time, freq)
        if len(cal) == 0:
            return pd.DataFrame(
                index=pd.MultiIndex.from_arrays([[], []], names=("instrument", "datetime")), columns=fields
            )
        data = {}
        for inst in inst_list:
            storage = self.backend_obj(instrument=inst, freq=freq)
            df = storage.read(fields, cal[0], cal[-1])
            df = df.reindex(cal)
            df.index.name = "datetime"
            data[inst] = df
        return pd.concat(data, names=["instrument"])

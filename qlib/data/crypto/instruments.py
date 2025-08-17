"""Crypto instrument provider.

This module provides :class:`CryptoInstrumentProvider` which extends
:class:`~qlib.data.data.InstrumentProvider` for the cryptocurrency
market.  Instruments can be loaded from JSON/CSV files or directly
from exchange HTTP APIs.  The provider also supports simple filters
such as minimal volume and keeps an in-memory cache similar to
``LocalInstrumentProvider``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

from ..data import InstrumentProvider
from ..cache import H


class CryptoInstrumentProvider(InstrumentProvider):
    """Instrument provider for crypto markets.

    Parameters
    ----------
    source : str
        Path to a JSON/CSV file or URL of an exchange API returning
        the instruments information.
    min_volume : float, optional
        Minimal volume used to filter instruments.  If the loaded
        data do not contain a ``volume`` column this filter is ignored.
    """

    def __init__(self, source: str, min_volume: float = 0.0) -> None:
        super().__init__()
        self.source = source
        self.min_volume = min_volume

    # ---------------------------------------------------------------------
    # loading utilities
    def _load_from_file(self, path: Path) -> pd.DataFrame:
        if path.suffix == ".json":
            return pd.read_json(path)
        if path.suffix == ".csv":
            return pd.read_csv(path)
        raise ValueError(f"Unsupported file format: {path.suffix}")

    def _load_from_api(self, url: str) -> pd.DataFrame:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        # normalise common payloads
        if isinstance(data, dict):
            if "data" in data:
                data = data["data"]
            elif "symbols" in data:
                data = data["symbols"]
        return pd.DataFrame(data)

    def _load_instruments(self, market: str, freq: str = "day") -> pd.DataFrame:  # pylint: disable=unused-argument
        path = Path(self.source)
        if path.exists():
            df = self._load_from_file(path)
        else:
            df = self._load_from_api(self.source)
        # Ensure symbol column exists
        if "symbol" not in df.columns:
            df = df.rename(columns={df.columns[0]: "symbol"})
        return df

    # ------------------------------------------------------------------
    def list_instruments(
        self,
        instruments: Dict,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        freq: str = "day",
        as_list: bool = False,
    ) -> Dict[str, List] | List[str]:
        """List instruments applying volume filters and caching.

        Parameters are the same as
        :py:meth:`~qlib.data.data.InstrumentProvider.list_instruments`.
        """

        market = instruments["market"]
        cache_key = f"crypto::{market}"
        if cache_key in H["i"]:
            df = H["i"][cache_key]
        else:
            df = self._load_instruments(market, freq=freq)
            H["i"][cache_key] = df

        # Apply minimal volume filter if possible
        if self.min_volume and "volume" in df.columns:
            df = df[df["volume"] >= self.min_volume]

        symbols = df["symbol"].tolist()

        if as_list:
            return symbols

        # For crypto instruments we do not have specific life spans.
        # Use the requested time range for all instruments.
        start = pd.Timestamp(start_time) if start_time else pd.Timestamp.min
        end = pd.Timestamp(end_time) if end_time else pd.Timestamp.max
        return {sym: [(start, end)] for sym in symbols}

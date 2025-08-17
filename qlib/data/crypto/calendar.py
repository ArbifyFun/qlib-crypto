"""Calendar provider for crypto markets.

This provider generates continuous timestamps for both daily and intraday
frequencies using :func:`pandas.date_range`.  Unlike equity markets the
crypto market trades 24/7, so weekends are not filtered out.
"""
from __future__ import annotations

from typing import List

import pandas as pd

from qlib.data.data import CalendarProvider as BaseCalendarProvider


class CalendarProvider(BaseCalendarProvider):
    """Calendar provider for crypto data."""

    START = pd.Timestamp("2000-01-01")

    def load_calendar(self, freq: str, future: bool) -> List[pd.Timestamp]:
        """Generate calendar timestamps.

        Parameters
        ----------
        freq : str
            Frequency string, e.g. ``"day"`` or ``"1min"``.
        future : bool
            If True, include future dates up to one year from today.
        """
        if freq in ("day", "1d"):
            pd_freq = "D"
        else:
            pd_freq = freq

        end = pd.Timestamp.today()
        if future:
            end += pd.Timedelta(days=365)

        calendar = pd.date_range(start=self.START, end=end, freq=pd_freq)
        return calendar.to_list()

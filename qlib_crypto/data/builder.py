from pathlib import Path
from typing import List

import fire
import pandas as pd

from .calendar import write_calendar
from .instruments import CryptoInstrument, dump_instruments_all


def _iter_csv_files(source_dir: Path) -> List[Path]:
    return sorted(source_dir.expanduser().resolve().glob("*.csv"))


def build_qlib_files(
    source_dir: str,
    calendar_path: str,
    instruments_path: str,
    exchange: str,
    market_type: str = "spot",
    default_start: str = "2009-01-01",
    default_end: str = "2099-12-31",
):
    """Build calendar and instruments files from normalized crypto csv files."""
    src = Path(source_dir).expanduser().resolve()
    files = _iter_csv_files(src)
    if not files:
        raise ValueError(f"no csv files found in {src}")

    global_min = None
    global_max = None
    instruments = []

    for fp in files:
        df = pd.read_csv(fp, usecols=["date", "symbol"])
        if df.empty:
            continue
        dt = pd.to_datetime(df["date"], utc=True)
        cur_min, cur_max = dt.min(), dt.max()
        global_min = cur_min if global_min is None else min(global_min, cur_min)
        global_max = cur_max if global_max is None else max(global_max, cur_max)

        listed_at = cur_min.strftime("%Y-%m-%d") if pd.notna(cur_min) else default_start
        delisted_at = cur_max.strftime("%Y-%m-%d") if pd.notna(cur_max) else default_end
        symbol = str(df["symbol"].iloc[0])
        instruments.append(
            CryptoInstrument(
                symbol=symbol,
                exchange=exchange,
                market_type=market_type,
                listed_at=listed_at,
                delisted_at=delisted_at,
            )
        )

    if global_min is None or global_max is None:
        raise ValueError("cannot infer calendar range from source csv files")

    write_calendar(
        output_path=calendar_path,
        start=global_min.strftime("%Y-%m-%d"),
        end=global_max.strftime("%Y-%m-%d"),
        freq="1D",
    )
    dump_instruments_all(instruments=instruments, output_path=instruments_path)


if __name__ == "__main__":
    fire.Fire(build_qlib_files)

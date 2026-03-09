from pathlib import Path

import pandas as pd


def generate_24_7_calendar(start: str, end: str, freq: str = "1D") -> pd.DatetimeIndex:
    """Generate a continuous 24/7 UTC calendar."""
    return pd.date_range(start=start, end=end, freq=freq, tz="UTC")


def write_calendar(output_path: str, start: str, end: str, freq: str = "1D", fmt: str = "%Y-%m-%d") -> Path:
    """Write qlib calendar txt (one timestamp per line)."""
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    cal = generate_24_7_calendar(start=start, end=end, freq=freq)
    output.write_text("\n".join(ts.strftime(fmt) for ts in cal) + "\n")
    return output


def write_day_calendar(output_path: str, start: str, end: str) -> Path:
    return write_calendar(output_path=output_path, start=start, end=end, freq="1D", fmt="%Y-%m-%d")


def write_1min_calendar(output_path: str, start: str, end: str) -> Path:
    return write_calendar(output_path=output_path, start=start, end=end, freq="1min", fmt="%Y-%m-%d %H:%M:%S")

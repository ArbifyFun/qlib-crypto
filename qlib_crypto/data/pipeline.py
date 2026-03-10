from pathlib import Path

import fire
import pandas as pd

from scripts.dump_bin import DumpDataAll
from qlib_crypto.data.builder import build_qlib_files


def _infer_exclude_fields(normalize_path: Path) -> str:
    """Infer non-numeric fields and always exclude string identifiers."""
    excluded = {"symbol", "exchange"}
    for csv_file in sorted(normalize_path.glob("*.csv")):
        try:
            sample = pd.read_csv(csv_file, nrows=200)
        except Exception:
            continue
        non_numeric = sample.select_dtypes(exclude=["number", "bool"]).columns
        excluded.update(non_numeric)

    return ",".join(sorted(excluded))
NON_NUMERIC_DUMP_FIELDS = "symbol,exchange"


def build_and_dump(
    normalize_dir: str,
    qlib_dir: str,
    exchange: str,
    market_type: str = "spot",
    freq: str = "day",
    max_workers: int = 1,
):
    """Build calendar/instruments and dump feature bins from normalized csv.

    Steps:
    1) generate calendars/<freq>.txt and instruments/all.txt from normalized csv
    2) dump csv fields into qlib bin layout using scripts.dump_bin.DumpDataAll
    """
    normalize_path = Path(normalize_dir).expanduser().resolve()
    qlib_path = Path(qlib_dir).expanduser().resolve()
    qlib_path.mkdir(parents=True, exist_ok=True)

    cal_path = qlib_path / "calendars" / f"{freq}.txt"
    ins_path = qlib_path / "instruments" / "all.txt"

    build_qlib_files(
        source_dir=str(normalize_path),
        calendar_path=str(cal_path),
        instruments_path=str(ins_path),
        exchange=exchange,
        market_type=market_type,
    )

    DumpDataAll(
        data_path=str(normalize_path),
        qlib_dir=str(qlib_path),
        freq=freq,
        max_workers=max_workers,
        date_field_name="date",
        symbol_field_name="symbol",
        exclude_fields=NON_NUMERIC_DUMP_FIELDS,
    ).dump()


if __name__ == "__main__":
    fire.Fire(build_and_dump)

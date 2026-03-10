from pathlib import Path

import fire

from scripts.dump_bin import DumpDataAll
from qlib_crypto.data.builder import build_qlib_files


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
        exclude_fields="symbol,exchange",
    ).dump()


if __name__ == "__main__":
    fire.Fire(build_and_dump)

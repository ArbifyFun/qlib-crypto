from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import fire


REQUIRED_CORE_FIELDS = ["close", "open", "high", "low", "volume", "factor"]


def _read_instruments(path: Path) -> List[str]:
    rows = [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]
    symbols = [r.split("\t")[0].lower() for r in rows if "\t" in r]
    return symbols


def check_qlib_crypto_dataset(qlib_dir: str, freq: str = "day", required_fields: List[str] = None) -> Dict:
    """Validate qlib crypto data layout and required feature bins.

    Returns a summary dict for programmatic checks.
    """
    required_fields = REQUIRED_CORE_FIELDS if required_fields is None else required_fields

    root = Path(qlib_dir).expanduser().resolve()
    cal = root / "calendars" / f"{freq}.txt"
    ins = root / "instruments" / "all.txt"
    feat = root / "features"

    summary = {
        "qlib_dir": str(root),
        "calendar_exists": cal.exists(),
        "calendar_lines": 0,
        "instruments_exists": ins.exists(),
        "instrument_count": 0,
        "feature_dir_exists": feat.exists(),
        "checked_symbols": 0,
        "missing": {},
        "ok": False,
    }

    if cal.exists():
        summary["calendar_lines"] = len([ln for ln in cal.read_text().splitlines() if ln.strip()])

    symbols: List[str] = []
    if ins.exists():
        symbols = _read_instruments(ins)
        summary["instrument_count"] = len(symbols)

    if feat.exists() and symbols:
        for sym in symbols:
            sdir = feat / sym
            miss = []
            for f in required_fields:
                p = sdir / f"{f}.{freq}.bin"
                if not p.exists():
                    miss.append(str(p.relative_to(root)))
            if miss:
                summary["missing"][sym] = miss
        summary["checked_symbols"] = len(symbols)

    summary["ok"] = (
        summary["calendar_exists"]
        and summary["instruments_exists"]
        and summary["feature_dir_exists"]
        and summary["calendar_lines"] > 0
        and summary["instrument_count"] > 0
        and len(summary["missing"]) == 0
    )
    return summary


def cli(qlib_dir: str, freq: str = "day"):
    report = check_qlib_crypto_dataset(qlib_dir=qlib_dir, freq=freq)
    print(report)
    if not report.get("ok", False):
        raise SystemExit(1)
    return report


if __name__ == "__main__":
    fire.Fire(cli)

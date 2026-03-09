from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.data import healthcheck as healthcheck_mod
from qlib_crypto.data.healthcheck import check_qlib_crypto_dataset


def test_healthcheck_pass(tmp_path: Path):
    root = tmp_path / "qlib"
    (root / "calendars").mkdir(parents=True)
    (root / "instruments").mkdir(parents=True)
    (root / "features" / "binance_btcusdt").mkdir(parents=True)

    (root / "calendars/day.txt").write_text("2024-01-01\n")
    (root / "instruments/all.txt").write_text("BINANCE_BTCUSDT\t2024-01-01\t2099-12-31\n")

    for f in ["close", "open", "high", "low", "volume", "factor"]:
        (root / "features" / "binance_btcusdt" / f"{f}.day.bin").write_bytes(b"00")

    rep = check_qlib_crypto_dataset(str(root), freq="day")
    assert rep["ok"] is True
    assert rep["instrument_count"] == 1


def test_healthcheck_detect_missing(tmp_path: Path):
    root = tmp_path / "qlib"
    (root / "calendars").mkdir(parents=True)
    (root / "instruments").mkdir(parents=True)
    (root / "features" / "binance_btcusdt").mkdir(parents=True)

    (root / "calendars/day.txt").write_text("2024-01-01\n")
    (root / "instruments/all.txt").write_text("BINANCE_BTCUSDT\t2024-01-01\t2099-12-31\n")
    (root / "features" / "binance_btcusdt" / "close.day.bin").write_bytes(b"00")

    rep = check_qlib_crypto_dataset(str(root), freq="day")
    assert rep["ok"] is False
    assert "binance_btcusdt" in rep["missing"]



def test_healthcheck_cli_exits_nonzero_when_invalid(tmp_path: Path):
    root = tmp_path / "qlib"
    root.mkdir(parents=True)
    try:
        healthcheck_mod.cli(str(root), freq="day")
        assert False, "cli should exit non-zero for invalid dataset"
    except SystemExit as e:
        assert e.code == 1

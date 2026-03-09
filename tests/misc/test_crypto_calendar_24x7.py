from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.data.calendar import generate_24_7_calendar, write_1min_calendar


def test_generate_24_7_minute_calendar_count():
    cal = generate_24_7_calendar("2024-01-01 00:00:00", "2024-01-01 00:02:00", freq="1min")
    assert len(cal) == 3


def test_write_1min_calendar(tmp_path: Path):
    p = write_1min_calendar(str(tmp_path / "1min.txt"), "2024-01-01 00:00:00", "2024-01-01 00:01:00")
    lines = p.read_text().strip().splitlines()
    assert lines == ["2024-01-01 00:00:00", "2024-01-01 00:01:00"]

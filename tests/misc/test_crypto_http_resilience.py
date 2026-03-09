from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.collector.http import build_retry_session


def test_retry_session_has_adapters_and_agent():
    session = build_retry_session(total=3)
    assert "https://" in session.adapters
    assert "http://" in session.adapters
    assert "qlib-crypto-collector" in session.headers.get("User-Agent", "")


def test_collectors_use_retry_session_by_source_text():
    meta_text = (ROOT / "qlib_crypto/collector/crypto_meta.py").read_text()
    ohlcv_text = (ROOT / "scripts/data_collector/crypto_ohlcv/collector.py").read_text()

    assert "build_retry_session" in meta_text
    assert "self._session.get" in meta_text
    assert "build_retry_session" in ohlcv_text
    assert "self._session.get" in ohlcv_text

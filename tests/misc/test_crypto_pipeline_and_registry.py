from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.data.symbol_registry import SymbolRegistry
from qlib_crypto.data import pipeline as pipeline_mod


def test_symbol_registry_roundtrip():
    s = SymbolRegistry.to_qlib_symbol("okx", "BTC-USDT-SWAP")
    assert s == "OKX_BTC_USDT_SWAP"
    ex, raw = SymbolRegistry.from_qlib_symbol(s)
    assert ex == "OKX"
    assert raw == "BTC-USDT-SWAP"


def test_pipeline_invokes_builder_and_dump(monkeypatch, tmp_path: Path):
    calls = {"builder": 0, "dump": 0, "dump_kwargs": None}

    def fake_builder(**kwargs):
        calls["builder"] += 1

    class FakeDump:
        def __init__(self, **kwargs):
            calls["dump_kwargs"] = kwargs

        def dump(self):
            calls["dump"] += 1

    monkeypatch.setattr(pipeline_mod, "build_qlib_files", fake_builder)
    monkeypatch.setattr(pipeline_mod, "DumpDataAll", FakeDump)

    pipeline_mod.build_and_dump(
        normalize_dir=str(tmp_path / "norm"),
        qlib_dir=str(tmp_path / "qlib"),
        exchange="binance",
    )

    assert calls["builder"] == 1
    assert calls["dump"] == 1
    assert calls["dump_kwargs"]["exclude_fields"] == "symbol,exchange"

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


def test_docs_exist():
    assert (ROOT / "qlib_crypto/README.md").exists()
    assert (ROOT / "qlib_crypto/MIGRATION.md").exists()


def test_lazy_exports_available():
    import qlib_crypto.contrib.data as data_mod

    assert "Alpha158Crypto" in data_mod.__all__
    assert "StablecoinNeutralize" in data_mod.__all__
    # trigger lazy import for lightweight processor path
    proc_cls = data_mod.StablecoinNeutralize
    assert proc_cls.__name__ == "StablecoinNeutralize"



def test_crypto_ci_uses_preflight_command():
    workflow = (ROOT / ".github/workflows/crypto_plugin_tests.yml").read_text(encoding="utf-8")
    assert "python -m qlib_crypto.release_preflight" in workflow

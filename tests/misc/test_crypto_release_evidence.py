from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.release_evidence import generate_release_evidence


def test_generate_release_evidence_file(tmp_path: Path):
    out = tmp_path / "evidence.md"
    rc = generate_release_evidence(output=str(out), strict_runtime=False, run_workflows=False)
    text = out.read_text(encoding="utf-8")
    assert "# qlib-crypto Release Evidence" in text
    assert "Command Results" in text
    assert rc in (0, 1)

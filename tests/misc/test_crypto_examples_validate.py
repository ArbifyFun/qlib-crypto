from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.examples.validate_configs import validate_example_configs


def test_validate_example_configs_passes_repo_examples():
    errors = validate_example_configs(str(ROOT / "qlib_crypto/examples"))
    assert errors == {}



def test_validate_example_configs_detects_missing_1min_freq(tmp_path: Path):
    bad = tmp_path / "bad_1min.yaml"
    bad.write_text(
        """
qlib_init:
  provider_uri: ~/.qlib/crypto_data_1min
  region: crypto
task:
  model: {class: LGBModel}
  dataset:
    kwargs:
      handler:
        kwargs:
          instruments: all
"""
    )
    errors = validate_example_configs(str(tmp_path))
    assert "bad_1min.yaml" in errors
    assert any("freq=1min" in e for e in errors["bad_1min.yaml"])

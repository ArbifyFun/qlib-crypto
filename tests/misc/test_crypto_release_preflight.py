from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from qlib_crypto.release_preflight import WORKFLOW_CONFIGS, WORKFLOW_URI_FOLDER, build_commands


def test_build_commands_default():
    cmds = build_commands(strict_runtime=False)
    assert len(cmds) == 2
    assert cmds[0][2:] == ["qlib_crypto.examples.validate_configs"]
    assert "test_crypto_integration_provider.py" in " ".join(cmds[1])


def test_build_commands_strict_runtime_adds_gate():
    cmds = build_commands(strict_runtime=True)
    assert len(cmds) == 3
    assert "QLIB_CRYPTO_STRICT_RUNTIME=1" in cmds[1][2]


def test_build_commands_with_qlib_dir_healthcheck():
    cmds = build_commands(qlib_dir="~/.qlib/crypto_data")
    assert cmds[1][:3] == [sys.executable, "-m", "qlib_crypto.data.healthcheck"]


def test_build_commands_with_workflows():
    cmds = build_commands(run_workflows=True)
    workflow_cmds = [c for c in cmds if c and c[:3] == [sys.executable, "-m", "qlib.cli.run"]]
    assert [c[3] for c in workflow_cmds] == WORKFLOW_CONFIGS
    assert all(c[4] == f"--uri_folder={WORKFLOW_URI_FOLDER}" for c in workflow_cmds)

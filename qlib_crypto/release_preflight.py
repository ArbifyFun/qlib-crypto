from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Callable, List, Sequence

import fire


WORKFLOW_URI_FOLDER = "mlruns_crypto_preflight"

WORKFLOW_CONFIGS = [
    "qlib_crypto/examples/workflow_config_lightgbm_alpha158_crypto.yaml",
    "qlib_crypto/examples/crypto_perp_daily.yaml",
    "qlib_crypto/examples/crypto_spot_1min.yaml",
]


def _run(cmd: Sequence[str], runner: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> int:
    print("+", " ".join(cmd))
    cp = runner(cmd, check=False)
    return cp.returncode


def build_commands(
    strict_runtime: bool = False,
    qlib_dir: str | None = None,
    run_workflows: bool = False,
) -> List[List[str]]:
    cmds: List[List[str]] = [[sys.executable, "-m", "qlib_crypto.examples.validate_configs"]]

    if qlib_dir:
        resolved = str(Path(qlib_dir).expanduser())
        cmds.append([sys.executable, "-m", "qlib_crypto.data.healthcheck", "--qlib_dir", resolved])

    if strict_runtime:
        cmds.append(
            [
                "bash",
                "-lc",
                "QLIB_CRYPTO_STRICT_RUNTIME=1 pytest -q tests/misc/test_crypto_integration_provider.py",
            ]
        )

    cmds.append(
        [
            "pytest",
            "-q",
            "tests/misc/test_crypto_examples_validate.py",
            "tests/misc/test_crypto_docs_and_exports.py",
            "tests/misc/test_crypto_http_resilience.py",
            "tests/misc/test_crypto_healthcheck.py",
            "tests/misc/test_crypto_pipeline_and_registry.py",
            "tests/misc/test_crypto_integration_provider.py",
            "-rs",
        ]
    )

    if run_workflows:
        for cfg in WORKFLOW_CONFIGS:
            cmds.append([sys.executable, "-m", "qlib.cli.run", cfg, f"--uri_folder={WORKFLOW_URI_FOLDER}"])

    return cmds


def main(
    strict_runtime: bool = False,
    qlib_dir: str | None = None,
    run_workflows: bool = False,
) -> None:
    for cmd in build_commands(strict_runtime=strict_runtime, qlib_dir=qlib_dir, run_workflows=run_workflows):
        rc = _run(cmd)
        if rc != 0:
            raise SystemExit(rc)
    print("release preflight checks passed")


if __name__ == "__main__":
    fire.Fire(main)

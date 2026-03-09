from __future__ import annotations

import datetime as dt
import subprocess
from pathlib import Path
from typing import List, Sequence, Tuple

import fire

from qlib_crypto.release_preflight import build_commands


def _run(cmd: Sequence[str]) -> Tuple[int, str]:
    cp = subprocess.run(cmd, check=False, text=True, capture_output=True)
    out = (cp.stdout or "") + (cp.stderr or "")
    return cp.returncode, out.strip()


def _emoji(rc: int) -> str:
    return "✅" if rc == 0 else "❌"


def generate_release_evidence(
    output: str = "release_evidence.md",
    qlib_dir: str | None = None,
    strict_runtime: bool = False,
    run_workflows: bool = False,
) -> int:
    cmds: List[List[str]] = build_commands(
        strict_runtime=strict_runtime,
        qlib_dir=qlib_dir,
        run_workflows=run_workflows,
    )
    results = []
    overall = 0
    for cmd in cmds:
        rc, out = _run(cmd)
        if rc != 0:
            overall = rc
        results.append((cmd, rc, out))

    p = Path(output)
    lines = [
        "# qlib-crypto Release Evidence",
        "",
        f"Generated: {dt.datetime.utcnow().isoformat()}Z",
        "",
        "## Parameters",
        f"- strict_runtime: `{strict_runtime}`",
        f"- qlib_dir: `{qlib_dir}`",
        f"- run_workflows: `{run_workflows}`",
        "",
        "## Command Results",
    ]
    for cmd, rc, out in results:
        lines.append(f"- {_emoji(rc)} `{ ' '.join(cmd) }` (exit={rc})")
        if out:
            lines.append("  <details><summary>output</summary>")
            lines.append("")
            lines.append("  ```text")
            for ln in out.splitlines():
                lines.append(f"  {ln}")
            lines.append("  ```")
            lines.append("  </details>")

    lines.extend(["", f"Overall exit code: `{overall}`", ""])
    p.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote release evidence to {p}")
    return overall


def main(
    output: str = "release_evidence.md",
    qlib_dir: str | None = None,
    strict_runtime: bool = False,
    run_workflows: bool = False,
) -> None:
    rc = generate_release_evidence(
        output=output,
        qlib_dir=qlib_dir,
        strict_runtime=strict_runtime,
        run_workflows=run_workflows,
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    fire.Fire(main)

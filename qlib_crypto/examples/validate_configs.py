from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import fire
import yaml


REQUIRED_TOP_LEVEL = ["qlib_init"]


def _load_yaml(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data or {}


def validate_example_configs(example_dir: str = None) -> Dict[str, List[str]]:
    base = Path(example_dir).expanduser().resolve() if example_dir else Path(__file__).resolve().parent
    files = sorted([p for p in base.glob("*.yaml") if p.is_file()])

    errors: Dict[str, List[str]] = {}
    for p in files:
        cur = []
        data = _load_yaml(p)
        for key in REQUIRED_TOP_LEVEL:
            if key not in data:
                cur.append(f"missing top-level key: {key}")

        if "task" in data:
            task = data.get("task", {})
            if not isinstance(task, dict):
                cur.append("task should be mapping")
            else:
                for key in ["model", "dataset"]:
                    if key not in task:
                        cur.append(f"task missing key: {key}")

                # minute workflow should explicitly set handler freq to avoid falling back to day.
                qlib_init = data.get("qlib_init", {}) if isinstance(data.get("qlib_init", {}), dict) else {}
                provider_uri = str(qlib_init.get("provider_uri", ""))
                if "1min" in provider_uri:
                    dataset_cfg = task.get("dataset", {})
                    handler_cfg = (
                        dataset_cfg.get("kwargs", {}).get("handler", {}).get("kwargs", {})
                        if isinstance(dataset_cfg, dict)
                        else {}
                    )
                    if handler_cfg.get("freq") != "1min":
                        cur.append("1min provider requires task.dataset.handler.kwargs.freq=1min")

        if cur:
            errors[p.name] = cur

    return errors


def cli(example_dir: str = None):
    errors = validate_example_configs(example_dir=example_dir)
    if errors:
        print(errors)
        raise SystemExit(1)
    print("all example yaml files are valid")


if __name__ == "__main__":
    fire.Fire(cli)

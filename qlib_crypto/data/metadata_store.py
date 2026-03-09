from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


class MetadataStore:
    """Simple JSON metadata persistence for symbol rules and exchange snapshots."""

    def __init__(self, path: str):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text())

    def save(self, payload: Dict[str, Any]) -> None:
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))

    def upsert(self, key: str, value: Any) -> Dict[str, Any]:
        data = self.load()
        data[key] = value
        self.save(data)
        return data

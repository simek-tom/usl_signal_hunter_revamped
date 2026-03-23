"""
Local settings store — persists key/value pairs to a JSON file on disk.

File location: backend/settings_local.json (gitignored, never committed).
Fallback: returns {} if the file doesn't exist yet (first run).
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

_FILE = Path(__file__).resolve().parent.parent.parent / "settings_local.json"
_lock = threading.Lock()


def read_all() -> dict[str, Any]:
    if not _FILE.exists():
        return {}
    with _lock:
        try:
            return json.loads(_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}


def write_all(data: dict[str, Any]) -> None:
    with _lock:
        _FILE.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
        )


def get(key: str) -> Any:
    return read_all().get(key)


def set_value(key: str, value: Any) -> None:
    data = read_all()
    data[key] = value
    write_all(data)


def delete(key: str) -> None:
    data = read_all()
    data.pop(key, None)
    write_all(data)

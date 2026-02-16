from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Dict, List

from .schemas import KafkaUser

_DATA_PATH = Path(__file__).parent / "data" / "users.json"
_LOCK = threading.Lock()


def _read_raw() -> List[Dict[str, str]]:
    if not _DATA_PATH.exists():
        return []
    with _DATA_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_raw(rows: List[Dict[str, str]]) -> None:
    _DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _DATA_PATH.open("w", encoding="utf-8") as handle:
        json.dump(rows, handle, indent=2)


def list_users() -> List[KafkaUser]:
    with _LOCK:
        return [KafkaUser(**row) for row in _read_raw()]


def upsert_user(user: KafkaUser) -> None:
    with _LOCK:
        rows = _read_raw()
        existing = {row["username"]: row for row in rows}
        existing[user.username] = user.model_dump()
        _write_raw(list(existing.values()))


def delete_user(username: str) -> bool:
    with _LOCK:
        rows = _read_raw()
        filtered = [row for row in rows if row.get("username") != username]
        if len(filtered) == len(rows):
            return False
        _write_raw(filtered)
        return True

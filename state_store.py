# state_store.py
from __future__ import annotations

import json
import os
import pickle
import tempfile
from typing import Any, TypeVar

T = TypeVar("T")


def _ensure_dir(path: str) -> str:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    return d


def atomic_write_bytes(path: str, data: bytes) -> None:
    """Write bytes atomically: write to temp file, fsync, then replace."""
    d = _ensure_dir(path)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def atomic_write_json(path: str, obj: Any, *, indent: int = 2) -> None:
    atomic_write_bytes(path, json.dumps(obj, ensure_ascii=False, indent=indent).encode("utf-8"))


def atomic_read_json(path: str, default: T) -> T:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def atomic_write_pickle(path: str, obj: Any) -> None:
    atomic_write_bytes(path, pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))


def atomic_read_pickle(path: str, default: T) -> T:
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except Exception:
        return default

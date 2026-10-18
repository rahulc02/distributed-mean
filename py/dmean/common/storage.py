"""Object storage port and the local-volume adapter.

Keys look like 'files/{job}/f17'. Writes are atomic (temp file + rename), so a
reader sees either the old object or the new one, never half of one. That is
what makes duplicate task executions harmless: they overwrite identical bytes.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Protocol


class ObjectStore(Protocol):
    def get(self, key: str) -> bytearray: ...
    def put(self, key: str, data: bytes) -> None: ...
    def delete_prefix(self, prefix: str) -> None: ...


class LocalStore:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root)

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root.resolve() not in path.parents:
            raise ValueError(f"key escapes the store root: {key!r}")
        return path

    def get(self, key: str) -> bytearray:
        """A writable buffer, so decoded arrays can be added into in place."""
        path = self._path(key)
        buf = bytearray(path.stat().st_size)
        with open(path, "rb", buffering=0) as f:
            view, filled = memoryview(buf), 0
            while filled < len(buf):
                n = f.readinto(view[filled:])
                if not n:
                    raise OSError(f"short read on {key}")
                filled += n
        return buf

    def put(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)                      # atomic on POSIX

    def delete_prefix(self, prefix: str) -> None:
        shutil.rmtree(self._path(prefix), ignore_errors=True)

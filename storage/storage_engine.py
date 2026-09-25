from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path


class StorageError(Exception):
    """Base class for storage-engine errors."""


class ObjectNotFoundError(StorageError):
    """Raised when an object/version does not exist."""


class ObjectAlreadyExistsError(StorageError):
    """Raised when an object/version already exists."""


class StorageFullError(StorageError):
    """Raised when there is not enough free capacity."""


@dataclass(frozen=True, slots=True)
class StorageStats:
    capacity_bytes: int
    used_bytes: int
    free_bytes: int


class StorageEngine:
    """Safe local filesystem storage for Vault object versions."""

    def __init__(self, data_dir: Path, capacity_bytes: int) -> None:
        self.data_dir = data_dir.resolve()
        self.capacity_bytes = capacity_bytes
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def object_path(self, object_id: str, version_id: str) -> Path:
        self._validate_id(object_id, "object_id")
        self._validate_id(version_id, "version_id")
        return self.data_dir / "objects" / object_id / version_id / "data"

    def exists(self, object_id: str, version_id: str) -> bool:
        return self.object_path(object_id, version_id).is_file()

    def write_bytes(self, object_id: str, version_id: str, data: bytes) -> int:
        path = self.object_path(object_id, version_id)
        if path.exists():
            raise ObjectAlreadyExistsError(f"Object version already exists: {object_id}/{version_id}")
        self._ensure_capacity(len(data))
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.parent / ".data.tmp"
        try:
            with temp_path.open("xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
        except FileExistsError as exc:
            raise ObjectAlreadyExistsError(
                f"Object version is being written or already exists: {object_id}/{version_id}"
            ) from exc
        finally:
            temp_path.unlink(missing_ok=True)
        return len(data)

    def read_bytes(self, object_id: str, version_id: str) -> bytes:
        path = self.object_path(object_id, version_id)
        if not path.is_file():
            raise ObjectNotFoundError(f"Object version not found: {object_id}/{version_id}")
        return path.read_bytes()

    def delete(self, object_id: str, version_id: str) -> None:
        path = self.object_path(object_id, version_id)
        if not path.is_file():
            raise ObjectNotFoundError(f"Object version not found: {object_id}/{version_id}")
        path.unlink()
        version_dir = path.parent
        object_dir = version_dir.parent
        try:
            version_dir.rmdir()
            object_dir.rmdir()
        except OSError:
            pass

    def stats(self) -> StorageStats:
        usage = shutil.disk_usage(self.data_dir)
        used_bytes = sum(p.stat().st_size for p in self.data_dir.rglob("*") if p.is_file())
        free_bytes = min(max(self.capacity_bytes - used_bytes, 0), usage.free)
        return StorageStats(self.capacity_bytes, used_bytes, free_bytes)

    def _ensure_capacity(self, incoming_bytes: int) -> None:
        stats = self.stats()
        if incoming_bytes > stats.free_bytes:
            raise StorageFullError(
                f"Insufficient storage capacity: required={incoming_bytes}, free={stats.free_bytes}"
            )

    @staticmethod
    def _validate_id(value: str, field_name: str) -> None:
        if not value or value in {".", ".."} or "/" in value or "\\" in value:
            raise ValueError(f"Invalid {field_name}")

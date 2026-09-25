from __future__ import annotations

import os
import shutil
import threading
import uuid
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
    """Safe local filesystem storage for Vault object versions.

    Step 2 hardening keeps filesystem mutations serialized within a node
    process, uses unique temporary files for atomic writes, validates IDs
    before constructing paths, and synchronizes completed filesystem changes.
    Large-object streaming is intentionally deferred to Step 3.
    """

    def __init__(self, data_dir: Path, capacity_bytes: int) -> None:
        if capacity_bytes <= 0:
            raise ValueError("capacity_bytes must be greater than zero")

        self.data_dir = data_dir.resolve()
        self.capacity_bytes = capacity_bytes
        self._lock = threading.RLock()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def object_path(self, object_id: str, version_id: str) -> Path:
        self._validate_id(object_id, "object_id")
        self._validate_id(version_id, "version_id")
        return self.data_dir / "objects" / object_id / version_id / "data"

    def exists(self, object_id: str, version_id: str) -> bool:
        with self._lock:
            return self.object_path(object_id, version_id).is_file()

    def write_bytes(self, object_id: str, version_id: str, data: bytes) -> int:
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")

        with self._lock:
            path = self.object_path(object_id, version_id)
            if path.exists():
                raise ObjectAlreadyExistsError(
                    f"Object version already exists: {object_id}/{version_id}"
                )

            self._ensure_capacity(len(data))
            path.parent.mkdir(parents=True, exist_ok=True)

            # Unique temporary files prevent unrelated concurrent writes from
            # colliding on a shared ".data.tmp" path.
            temp_path = path.parent / f".data.{uuid.uuid4().hex}.tmp"
            try:
                with temp_path.open("xb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())

                # The destination is only exposed after the complete payload
                # has been flushed. os.replace() is atomic on the same
                # filesystem, while the existence check above plus the
                # process lock prevents overwrite within this node process.
                os.replace(temp_path, path)
                self._fsync_directory(path.parent)
            except FileExistsError as exc:
                raise ObjectAlreadyExistsError(
                    f"Object version already exists: {object_id}/{version_id}"
                ) from exc
            finally:
                temp_path.unlink(missing_ok=True)

            return len(data)

    def read_bytes(self, object_id: str, version_id: str) -> bytes:
        with self._lock:
            path = self.object_path(object_id, version_id)
            if not path.is_file():
                raise ObjectNotFoundError(
                    f"Object version not found: {object_id}/{version_id}"
                )
            return path.read_bytes()

    def delete(self, object_id: str, version_id: str) -> None:
        with self._lock:
            path = self.object_path(object_id, version_id)
            if not path.is_file():
                raise ObjectNotFoundError(
                    f"Object version not found: {object_id}/{version_id}"
                )

            path.unlink()
            self._fsync_directory(path.parent)

            # Empty version/object directories are implementation details.
            # Remove them when possible, but never make deletion fail merely
            # because another version still exists.
            version_dir = path.parent
            object_dir = version_dir.parent
            try:
                version_dir.rmdir()
                object_dir.rmdir()
                self._fsync_directory(object_dir.parent)
            except OSError:
                pass

    def stats(self) -> StorageStats:
        with self._lock:
            usage = shutil.disk_usage(self.data_dir)
            used_bytes = sum(
                path.stat().st_size
                for path in self.data_dir.rglob("*")
                if path.is_file()
            )
            free_bytes = min(max(self.capacity_bytes - used_bytes, 0), usage.free)
            return StorageStats(
                capacity_bytes=self.capacity_bytes,
                used_bytes=used_bytes,
                free_bytes=free_bytes,
            )

    def _ensure_capacity(self, incoming_bytes: int) -> None:
        if incoming_bytes < 0:
            raise ValueError("incoming_bytes must not be negative")

        stats = self.stats()
        if incoming_bytes > stats.free_bytes:
            raise StorageFullError(
                f"Insufficient storage capacity: required={incoming_bytes}, free={stats.free_bytes}"
            )

    @staticmethod
    def _validate_id(value: str, field_name: str) -> None:
        if (
            not isinstance(value, str)
            or not value
            or value in {".", ".."}
            or "/" in value
            or "\\" in value
            or "\x00" in value
        ):
            raise ValueError(f"Invalid {field_name}")

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        """Best-effort directory fsync after a filesystem namespace change."""
        try:
            fd = os.open(directory, os.O_RDONLY)
        except OSError:
            return

        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)

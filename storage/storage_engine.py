from __future__ import annotations

import asyncio
import json
import os
import shutil
import threading
import uuid
from collections.abc import AsyncIterable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path


class StorageError(Exception):
    """Base class for storage-engine errors."""


class ObjectNotFoundError(StorageError):
    """Raised when an object/version does not exist."""


class ObjectAlreadyExistsError(StorageError):
    """Raised when an object/version already exists."""


class StorageFullError(StorageError):
    """Raised when there is not enough free storage capacity."""


@dataclass(frozen=True, slots=True)
class StorageStats:
    capacity_bytes: int
    used_bytes: int
    free_bytes: int


class StorageEngine:
    """Local chunked filesystem storage for Vault object versions.

    Step 3 stores objects as immutable chunks beneath a staging directory and
    publishes the complete version atomically. Request-body streaming keeps
    memory bounded by the configured chunk size rather than object size.
    """

    METADATA_NAME = "metadata.json"
    CHUNK_PREFIX = "chunk-"

    def __init__(self, data_dir: Path, capacity_bytes: int, chunk_size_bytes: int = 16 * 1024**2) -> None:
        if capacity_bytes <= 0:
            raise ValueError("capacity_bytes must be greater than zero")
        if chunk_size_bytes <= 0:
            raise ValueError("chunk_size_bytes must be greater than zero")

        self.data_dir = data_dir.resolve()
        self.capacity_bytes = capacity_bytes
        self.chunk_size_bytes = chunk_size_bytes
        self._lock = threading.RLock()
        self._stream_lock = asyncio.Lock()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def object_path(self, object_id: str, version_id: str) -> Path:
        """Return the published version directory."""
        self._validate_id(object_id, "object_id")
        self._validate_id(version_id, "version_id")
        return self.data_dir / "objects" / object_id / version_id

    def exists(self, object_id: str, version_id: str) -> bool:
        with self._lock:
            version_dir = self.object_path(object_id, version_id)
            return (version_dir / self.METADATA_NAME).is_file()

    def object_size(self, object_id: str, version_id: str) -> int:
        with self._lock:
            metadata = self._read_metadata(object_id, version_id)
            return int(metadata["size_bytes"])

    def write_bytes(self, object_id: str, version_id: str, data: bytes) -> int:
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        return self._write_iterable(object_id, version_id, (data,))

    async def write_stream(
        self,
        object_id: str,
        version_id: str,
        chunks: AsyncIterable[bytes],
    ) -> int:
        """Consume an async request stream without buffering the full object."""
        async with self._stream_lock:
            self._validate_ids(object_id, version_id)
            with self._lock:
                self._ensure_new_object(object_id, version_id)
                staging_dir = self._create_staging_dir(object_id, version_id)

            try:
                size = 0
                chunk_index = 0
                chunk_written = 0
                chunk_handle = None

                async for incoming in chunks:
                    if not isinstance(incoming, bytes):
                        raise TypeError("request stream must yield bytes")
                    if not incoming:
                        continue

                    offset = 0
                    while offset < len(incoming):
                        if chunk_handle is None:
                            chunk_path = staging_dir / self._chunk_name(chunk_index)
                            chunk_handle = chunk_path.open("xb")
                            chunk_written = 0

                        remaining = self.chunk_size_bytes - chunk_written
                        piece = incoming[offset : offset + remaining]
                        with self._lock:
                            self._ensure_capacity(len(piece))
                        chunk_handle.write(piece)
                        chunk_written += len(piece)
                        size += len(piece)
                        offset += len(piece)

                        if chunk_written == self.chunk_size_bytes:
                            chunk_handle.flush()
                            os.fsync(chunk_handle.fileno())
                            chunk_handle.close()
                            chunk_handle = None
                            chunk_index += 1

                if chunk_handle is not None:
                    chunk_handle.flush()
                    os.fsync(chunk_handle.fileno())
                    chunk_handle.close()
                    chunk_handle = None
                    chunk_index += 1

                metadata = {
                    "object_id": object_id,
                    "version_id": version_id,
                    "size_bytes": size,
                    "chunk_size_bytes": self.chunk_size_bytes,
                    "chunk_count": chunk_index,
                }
                self._write_metadata(staging_dir, metadata)
                self._publish_staging(object_id, version_id, staging_dir)
                return size
            except Exception:
                if chunk_handle is not None:
                    try:
                        chunk_handle.close()
                    except OSError:
                        pass
                self._remove_tree(staging_dir)
                raise

    def read_bytes(self, object_id: str, version_id: str) -> bytes:
        return b"".join(self.iter_chunks(object_id, version_id))

    def iter_chunks(self, object_id: str, version_id: str) -> Iterator[bytes]:
        """Yield stored chunks in order; only one chunk is held in memory."""
        with self._lock:
            metadata = self._read_metadata(object_id, version_id)
            chunk_count = int(metadata["chunk_count"])
            version_dir = self.object_path(object_id, version_id)
            chunk_paths = [
                version_dir / self._chunk_name(index)
                for index in range(chunk_count)
            ]

        for chunk_path in chunk_paths:
            with chunk_path.open("rb") as handle:
                while True:
                    data = handle.read(self.chunk_size_bytes)
                    if not data:
                        break
                    yield data

    def delete(self, object_id: str, version_id: str) -> None:
        with self._lock:
            version_dir = self.object_path(object_id, version_id)
            if not (version_dir / self.METADATA_NAME).is_file():
                raise ObjectNotFoundError(
                    f"Object version not found: {object_id}/{version_id}"
                )

            self._remove_tree(version_dir)
            self._fsync_directory(version_dir.parent)

            object_dir = version_dir.parent
            try:
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

    def _write_iterable(
        self,
        object_id: str,
        version_id: str,
        chunks: Iterable[bytes],
    ) -> int:
        self._validate_ids(object_id, version_id)
        with self._lock:
            self._ensure_new_object(object_id, version_id)
            staging_dir = self._create_staging_dir(object_id, version_id)

            try:
                size = 0
                chunk_index = 0
                chunk_buffer = bytearray()

                for incoming in chunks:
                    if not isinstance(incoming, bytes):
                        raise TypeError("data chunks must be bytes")
                    if not incoming:
                        continue

                    offset = 0
                    while offset < len(incoming):
                        remaining = self.chunk_size_bytes - len(chunk_buffer)
                        piece = incoming[offset : offset + remaining]
                        chunk_buffer.extend(piece)
                        size += len(piece)
                        offset += len(piece)

                        if len(chunk_buffer) == self.chunk_size_bytes:
                            self._ensure_capacity(size)
                            self._write_chunk(staging_dir, chunk_index, bytes(chunk_buffer))
                            chunk_buffer.clear()
                            chunk_index += 1

                if chunk_buffer:
                    self._ensure_capacity(size)
                    self._write_chunk(staging_dir, chunk_index, bytes(chunk_buffer))
                    chunk_index += 1

                metadata = {
                    "object_id": object_id,
                    "version_id": version_id,
                    "size_bytes": size,
                    "chunk_size_bytes": self.chunk_size_bytes,
                    "chunk_count": chunk_index,
                }
                self._write_metadata(staging_dir, metadata)
                self._publish_staging(object_id, version_id, staging_dir)
                return size
            except Exception:
                self._remove_tree(staging_dir)
                raise

    def _ensure_new_object(self, object_id: str, version_id: str) -> None:
        if self.exists(object_id, version_id):
            raise ObjectAlreadyExistsError(
                f"Object version already exists: {object_id}/{version_id}"
            )

    def _create_staging_dir(self, object_id: str, version_id: str) -> Path:
        version_parent = self.object_path(object_id, version_id).parent
        version_parent.mkdir(parents=True, exist_ok=True)
        staging_dir = version_parent / f".{version_id}.{uuid.uuid4().hex}.upload"
        staging_dir.mkdir()
        return staging_dir

    def _publish_staging(
        self,
        object_id: str,
        version_id: str,
        staging_dir: Path,
    ) -> None:
        final_dir = self.object_path(object_id, version_id)
        if final_dir.exists():
            raise ObjectAlreadyExistsError(
                f"Object version already exists: {object_id}/{version_id}"
            )
        os.replace(staging_dir, final_dir)
        self._fsync_directory(final_dir.parent)

    def _write_chunk(self, staging_dir: Path, index: int, data: bytes) -> None:
        chunk_path = staging_dir / self._chunk_name(index)
        with chunk_path.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())

    def _write_metadata(self, staging_dir: Path, metadata: dict[str, int | str]) -> None:
        metadata_path = staging_dir / self.METADATA_NAME
        temp_path = staging_dir / f".{self.METADATA_NAME}.{uuid.uuid4().hex}.tmp"
        try:
            with temp_path.open("x", encoding="utf-8") as handle:
                json.dump(metadata, handle, separators=(",", ":"), sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, metadata_path)
            self._fsync_directory(staging_dir)
        finally:
            temp_path.unlink(missing_ok=True)

    def _read_metadata(self, object_id: str, version_id: str) -> dict[str, object]:
        version_dir = self.object_path(object_id, version_id)
        metadata_path = version_dir / self.METADATA_NAME
        if not metadata_path.is_file():
            raise ObjectNotFoundError(
                f"Object version not found: {object_id}/{version_id}"
            )
        try:
            with metadata_path.open("r", encoding="utf-8") as handle:
                metadata = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise StorageError(
                f"Object metadata is unreadable: {object_id}/{version_id}"
            ) from exc
        return metadata

    def _ensure_capacity(self, required_bytes: int) -> None:
        if required_bytes < 0:
            raise ValueError("required_bytes must not be negative")

        stats = self.stats()
        if required_bytes > stats.free_bytes:
            raise StorageFullError(
                f"Insufficient storage capacity: required={required_bytes}, free={stats.free_bytes}"
            )

    @classmethod
    def _chunk_name(cls, index: int) -> str:
        return f"{cls.CHUNK_PREFIX}{index:06d}"

    @classmethod
    def _validate_ids(cls, object_id: str, version_id: str) -> None:
        cls._validate_id(object_id, "object_id")
        cls._validate_id(version_id, "version_id")

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
    def _remove_tree(path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
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

from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import uuid
from collections.abc import AsyncIterable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from .checksum import sha256_bytes


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


@dataclass(frozen=True, slots=True)
class VerificationResult:
    object_id: str
    version_id: str
    valid: bool
    checksum: str | None
    chunk_count: int
    corrupt_chunks: tuple[int, ...]
    errors: tuple[str, ...]


class StorageEngine:
    """Local chunked filesystem storage with streaming SHA-256 integrity."""

    METADATA_NAME = "metadata.json"
    CHUNK_PREFIX = "chunk-"
    MAX_VERIFY_CHUNKS = 1_000_000

    def __init__(
        self,
        data_dir: Path,
        capacity_bytes: int,
        chunk_size_bytes: int = 16 * 1024**2,
    ) -> None:
        if capacity_bytes <= 0:
            raise ValueError("capacity_bytes must be greater than zero")
        if chunk_size_bytes <= 0:
            raise ValueError("chunk_size_bytes must be greater than zero")

        self.data_dir = data_dir.resolve()
        self.capacity_bytes = capacity_bytes
        self.chunk_size_bytes = chunk_size_bytes
        self._lock = threading.RLock()
        self._reserved_bytes = 0

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._cleanup_staging_directories()

    def object_path(self, object_id: str, version_id: str) -> Path:
        self._validate_ids(object_id, version_id)
        return self.data_dir / "objects" / object_id / version_id

    def exists(self, object_id: str, version_id: str) -> bool:
        with self._lock:
            return (self.object_path(object_id, version_id) / self.METADATA_NAME).is_file()

    def object_size(self, object_id: str, version_id: str) -> int:
        with self._lock:
            metadata = self._read_metadata(object_id, version_id)
            try:
                return int(metadata["size_bytes"])
            except (KeyError, TypeError, ValueError) as exc:
                raise StorageError(
                    f"Object metadata has invalid size_bytes: {object_id}/{version_id}"
                ) from exc

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
        self._validate_ids(object_id, version_id)
            with self._lock:
                self._ensure_new_object(object_id, version_id)
                staging_dir = self._create_staging_dir(object_id, version_id)

            size = 0
            chunk_index = 0
            chunk_written = 0
            chunk_handle = None
            chunk_digests: list[str] = []
            object_digest = hashlib.sha256()
            chunk_digest = hashlib.sha256()
            reserved_bytes = 0

            try:
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
                            chunk_digest = hashlib.sha256()

                        remaining = self.chunk_size_bytes - chunk_written
                        piece = incoming[offset : offset + remaining]

                        with self._lock:
                            self._reserve_capacity(len(piece))
                            reserved_bytes += len(piece)

                        try:
                            chunk_handle.write(piece)
                        except Exception:
                            raise

                        chunk_digest.update(piece)
                        object_digest.update(piece)
                        chunk_written += len(piece)
                        size += len(piece)
                        offset += len(piece)

                        if chunk_written == self.chunk_size_bytes:
                            chunk_handle.flush()
                            os.fsync(chunk_handle.fileno())
                            chunk_handle.close()
                            chunk_handle = None
                            chunk_digests.append(chunk_digest.hexdigest())
                            chunk_index += 1

                if chunk_handle is not None:
                    chunk_handle.flush()
                    os.fsync(chunk_handle.fileno())
                    chunk_handle.close()
                    chunk_handle = None
                    chunk_digests.append(chunk_digest.hexdigest())
                    chunk_index += 1

                metadata = {
                    "object_id": object_id,
                    "version_id": version_id,
                    "size_bytes": size,
                    "chunk_size_bytes": self.chunk_size_bytes,
                    "chunk_count": chunk_index,
                    "checksum": object_digest.hexdigest(),
                    "chunk_checksums": chunk_digests,
                }
                self._write_metadata(staging_dir, metadata)
                self._publish_staging(object_id, version_id, staging_dir)
                with self._lock:
                    self._release_capacity(reserved_bytes)
                reserved_bytes = 0
                return size
            except Exception:
                if chunk_handle is not None:
                    try:
                        chunk_handle.close()
                    except OSError:
                        pass
                self._remove_tree(staging_dir)
                with self._lock:
                    self._release_capacity(reserved_bytes)
                raise

    def read_bytes(self, object_id: str, version_id: str) -> bytes:
        return b"".join(self.iter_chunks(object_id, version_id))

    def iter_chunks(self, object_id: str, version_id: str) -> Iterator[bytes]:
        with self._lock:
            metadata = self._read_metadata(object_id, version_id)
            try:
                chunk_count = int(metadata["chunk_count"])
                stored_chunk_size = int(metadata["chunk_size_bytes"])
            except (KeyError, TypeError, ValueError) as exc:
                raise StorageError(
                    f"Object metadata has invalid chunk layout: {object_id}/{version_id}"
                ) from exc
            if chunk_count < 0 or stored_chunk_size <= 0:
                raise StorageError(
                    f"Object metadata has invalid chunk layout: {object_id}/{version_id}"
                )
            version_dir = self.object_path(object_id, version_id)
            chunk_paths = [
                version_dir / self._chunk_name(index)
                for index in range(chunk_count)
            ]

        read_size = min(stored_chunk_size, self.chunk_size_bytes)
        for chunk_path in chunk_paths:
            if chunk_path.is_symlink():
                raise StorageError(f"Object chunk is a symlink: {chunk_path.name}")
            with chunk_path.open("rb") as handle:
                while True:
                    data = handle.read(read_size)
                    if not data:
                        break
                    yield data

    def verify(self, object_id: str, version_id: str) -> VerificationResult:
        """Recompute stored checksums and validate the complete on-disk layout."""
        self._validate_ids(object_id, version_id)

        try:
            with self._lock:
                metadata = self._read_metadata(object_id, version_id)
        except ObjectNotFoundError:
            raise
        except StorageError as exc:
            return VerificationResult(
                object_id,
                version_id,
                False,
                None,
                0,
                (),
                (str(exc),),
            )

        version_dir = self.object_path(object_id, version_id)
        errors: list[str] = []
        corrupt: set[int] = set()

        expected_object_id = metadata.get("object_id")
        expected_version_id = metadata.get("version_id")
        if expected_object_id != object_id:
            errors.append("metadata object_id mismatch")
        if expected_version_id != version_id:
            errors.append("metadata version_id mismatch")

        size_bytes = self._metadata_int(metadata, "size_bytes", errors)
        chunk_size_bytes = self._metadata_int(metadata, "chunk_size_bytes", errors)
        chunk_count = self._metadata_int(metadata, "chunk_count", errors)

        if size_bytes is not None and size_bytes < 0:
            errors.append("invalid size_bytes")
            size_bytes = None
        if chunk_size_bytes is not None and chunk_size_bytes <= 0:
            errors.append("invalid chunk_size_bytes")
            chunk_size_bytes = None
        if chunk_count is not None and chunk_count < 0:
            errors.append("invalid chunk_count")
            chunk_count = None
        if chunk_count is not None and chunk_count > self.MAX_VERIFY_CHUNKS:
            errors.append("chunk_count exceeds verification limit")
            chunk_count = None

        expected_checksum = metadata.get("checksum")
        if not self._is_sha256(expected_checksum):
            errors.append("invalid checksum metadata")
            expected_checksum = None

        expected_chunk_checksums = metadata.get("chunk_checksums")
        if not isinstance(expected_chunk_checksums, list):
            errors.append("invalid chunk_checksums metadata")
            expected_chunk_checksums = []
        else:
            for index, checksum in enumerate(expected_chunk_checksums):
                if not self._is_sha256(checksum):
                    errors.append(f"invalid checksum metadata for chunk {index}")

        if chunk_count is not None and len(expected_chunk_checksums) != chunk_count:
            errors.append("chunk_checksums length does not match chunk_count")

        if size_bytes is not None and chunk_size_bytes is not None and chunk_count is not None:
            expected_count = 0 if size_bytes == 0 else (size_bytes + chunk_size_bytes - 1) // chunk_size_bytes
            if expected_count != chunk_count:
                errors.append("chunk_count does not match size_bytes")

        try:
            entries = list(version_dir.iterdir())
        except OSError as exc:
            errors.append(f"cannot inspect object directory: {exc}")
            return VerificationResult(
                object_id,
                version_id,
                False,
                None,
                chunk_count or 0,
                tuple(),
                tuple(errors),
            )

        actual_chunk_names = {
            entry.name
            for entry in entries
            if entry.name.startswith(self.CHUNK_PREFIX)
        }
        expected_names = (
            {
                self._chunk_name(index)
                for index in range(chunk_count)
            }
            if chunk_count is not None and chunk_count >= 0
            else set()
        )
        for extra_name in sorted(actual_chunk_names - expected_names):
            errors.append(f"unexpected chunk {extra_name}")
            parsed = self._parse_chunk_index(extra_name)
            if parsed is not None:
                corrupt.add(parsed)

        object_digest = hashlib.sha256()
        actual_checksum: str | None = None
        actual_size = 0

        if chunk_count is not None and chunk_count >= 0:
            read_size = (
                min(chunk_size_bytes, self.chunk_size_bytes)
                if chunk_size_bytes
                else self.chunk_size_bytes
            )
            for index in range(chunk_count):
                path = version_dir / self._chunk_name(index)

                if path.is_symlink():
                    corrupt.add(index)
                    errors.append(f"chunk {index} is a symlink")
                    continue

                if not path.is_file():
                    corrupt.add(index)
                    errors.append(f"missing chunk {index}")
                    continue

                try:
                    stored_size = path.stat().st_size
                except OSError as exc:
                    corrupt.add(index)
                    errors.append(f"cannot stat chunk {index}: {exc}")
                    continue

                expected_size = self._expected_chunk_size(
                    size_bytes,
                    chunk_size_bytes,
                    chunk_count,
                    index,
                )
                if expected_size is not None and stored_size != expected_size:
                    corrupt.add(index)
                    errors.append(
                        f"chunk {index} size mismatch: expected={expected_size}, actual={stored_size}"
                    )

                digest = hashlib.sha256()
                try:
                    with path.open("rb") as handle:
                        while True:
                            data = handle.read(read_size)
                            if not data:
                                break
                            digest.update(data)
                            object_digest.update(data)
                            actual_size += len(data)
                except OSError as exc:
                    corrupt.add(index)
                    errors.append(f"unreadable chunk {index}: {exc}")
                    continue

                actual_chunk_checksum = digest.hexdigest()
                if index >= len(expected_chunk_checksums):
                    corrupt.add(index)
                    errors.append(f"missing checksum metadata for chunk {index}")
                elif actual_chunk_checksum != expected_chunk_checksums[index]:
                    corrupt.add(index)
                    errors.append(f"checksum mismatch for chunk {index}")

        actual_checksum = object_digest.hexdigest()

        if size_bytes is not None and actual_size != size_bytes:
            errors.append(
                f"object size mismatch: expected={size_bytes}, actual={actual_size}"
            )

        if expected_checksum is not None and actual_checksum != expected_checksum:
            errors.append("object checksum mismatch")

        valid = not errors and not corrupt
        return VerificationResult(
            object_id,
            version_id,
            valid,
            actual_checksum,
            chunk_count or 0,
            tuple(sorted(corrupt)),
            tuple(errors),
        )

    def delete(self, object_id: str, version_id: str) -> None:
        with self._lock:
            version_dir = self.object_path(object_id, version_id)
            if not (version_dir / self.METADATA_NAME).is_file():
                raise ObjectNotFoundError(
                    f"Object version not found: {object_id}/{version_id}"
                )

            self._remove_tree(version_dir)
            self._fsync_directory(version_dir.parent)

            try:
                version_dir.parent.rmdir()
                self._fsync_directory(version_dir.parent.parent)
            except OSError:
                pass

    def stats(self) -> StorageStats:
        with self._lock:
            usage = shutil.disk_usage(self.data_dir)
            used_bytes = sum(
                path.stat().st_size
                for path in self.data_dir.rglob("*")
                if path.is_file()
                and not path.is_symlink()
                and path.name.startswith(self.CHUNK_PREFIX)
                and not path.parent.name.endswith(".upload")
            )
            logical_free = max(
                self.capacity_bytes - used_bytes - self._reserved_bytes,
                0,
            )
            return StorageStats(
                self.capacity_bytes,
                used_bytes,
                min(logical_free, usage.free),
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

            size = 0
            chunk_index = 0
            chunk_buffer = bytearray()
            chunk_digests: list[str] = []
            object_digest = hashlib.sha256()
            reserved_bytes = 0

            try:
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
                        object_digest.update(piece)
                        size += len(piece)
                        offset += len(piece)

                        if len(chunk_buffer) == self.chunk_size_bytes:
                            self._reserve_capacity(len(chunk_buffer))
                            reserved_bytes += len(chunk_buffer)
                            data = bytes(chunk_buffer)
                            self._write_chunk(staging_dir, chunk_index, data)
                            chunk_digests.append(sha256_bytes(data))
                            chunk_buffer.clear()
                            chunk_index += 1

                if chunk_buffer:
                    self._reserve_capacity(len(chunk_buffer))
                    reserved_bytes += len(chunk_buffer)
                    data = bytes(chunk_buffer)
                    self._write_chunk(staging_dir, chunk_index, data)
                    chunk_digests.append(sha256_bytes(data))
                    chunk_index += 1

                metadata = {
                    "object_id": object_id,
                    "version_id": version_id,
                    "size_bytes": size,
                    "chunk_size_bytes": self.chunk_size_bytes,
                    "chunk_count": chunk_index,
                    "checksum": object_digest.hexdigest(),
                    "chunk_checksums": chunk_digests,
                }
                self._write_metadata(staging_dir, metadata)
                self._publish_staging(object_id, version_id, staging_dir)
                self._release_capacity(reserved_bytes)
                reserved_bytes = 0
                return size
            except Exception:
                self._remove_tree(staging_dir)
                self._release_capacity(reserved_bytes)
                raise

    def _ensure_new_object(self, object_id: str, version_id: str) -> None:
        if self.exists(object_id, version_id):
            raise ObjectAlreadyExistsError(
                f"Object version already exists: {object_id}/{version_id}"
            )

    def _create_staging_dir(self, object_id: str, version_id: str) -> Path:
        parent = self.object_path(object_id, version_id).parent
        parent.mkdir(parents=True, exist_ok=True)
        staging_dir = parent / f".{version_id}.{uuid.uuid4().hex}.upload"
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

    def _write_metadata(
        self,
        staging_dir: Path,
        metadata: dict[str, object],
    ) -> None:
        target = staging_dir / self.METADATA_NAME
        temp = staging_dir / f".{self.METADATA_NAME}.{uuid.uuid4().hex}.tmp"
        try:
            with temp.open("x", encoding="utf-8") as handle:
                json.dump(metadata, handle, separators=(",", ":"), sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, target)
            self._fsync_directory(staging_dir)
        finally:
            temp.unlink(missing_ok=True)

    def _read_metadata(
        self,
        object_id: str,
        version_id: str,
    ) -> dict[str, object]:
        version_dir = self.object_path(object_id, version_id)
        metadata_path = version_dir / self.METADATA_NAME

        if metadata_path.is_symlink():
            raise StorageError(
                f"Object metadata is a symlink: {object_id}/{version_id}"
            )

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

        if not isinstance(metadata, dict):
            raise StorageError(
                f"Object metadata is not a JSON object: {object_id}/{version_id}"
            )
        return metadata

    def _reserve_capacity(self, required_bytes: int) -> None:
        if required_bytes < 0:
            raise ValueError("required_bytes must not be negative")

        usage = shutil.disk_usage(self.data_dir)
        used_bytes = self._used_payload_bytes()
        logical_free = max(
            self.capacity_bytes - used_bytes - self._reserved_bytes,
            0,
        )
        free_bytes = min(logical_free, usage.free)

        if required_bytes > free_bytes:
            raise StorageFullError(
                f"Insufficient storage capacity: required={required_bytes}, free={free_bytes}"
            )

        self._reserved_bytes += required_bytes

    def _release_capacity(self, released_bytes: int) -> None:
        if released_bytes < 0:
            raise ValueError("released_bytes must not be negative")
        self._reserved_bytes = max(self._reserved_bytes - released_bytes, 0)

    def _used_payload_bytes(self) -> int:
        return sum(
            path.stat().st_size
            for path in self.data_dir.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and path.name.startswith(self.CHUNK_PREFIX)
            and not path.parent.name.endswith(".upload")
        )

    def _cleanup_staging_directories(self) -> None:
        objects_dir = self.data_dir / "objects"
        if not objects_dir.is_dir():
            return

        for object_dir in objects_dir.iterdir():
            if not object_dir.is_dir() or object_dir.is_symlink():
                continue
            for staging_dir in object_dir.glob(".*.upload"):
                self._remove_tree(staging_dir)

    @staticmethod
    def _metadata_int(
        metadata: dict[str, object],
        key: str,
        errors: list[str],
    ) -> int | None:
        value = metadata.get(key)
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(f"invalid {key}")
            return None
        return value

    @staticmethod
    def _is_sha256(value: object) -> bool:
        if not isinstance(value, str) or len(value) != 64:
            return False
        return value == value.lower() and all(
            character in "0123456789abcdef" for character in value
        )

    @classmethod
    def _expected_chunk_size(
        cls,
        size_bytes: int | None,
        chunk_size_bytes: int | None,
        chunk_count: int | None,
        index: int,
    ) -> int | None:
        if (
            size_bytes is None
            or chunk_size_bytes is None
            or chunk_count is None
            or chunk_count <= 0
            or index < 0
            or index >= chunk_count
        ):
            return None
        if index < chunk_count - 1:
            return chunk_size_bytes
        return size_bytes - (chunk_count - 1) * chunk_size_bytes

    @classmethod
    def _parse_chunk_index(cls, name: str) -> int | None:
        prefix = cls.CHUNK_PREFIX
        suffix = name[len(prefix) :]
        if len(suffix) != 6 or not suffix.isdigit():
            return None
        return int(suffix)

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
            or "\\"
            in value
            or "\x00"
            in value
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

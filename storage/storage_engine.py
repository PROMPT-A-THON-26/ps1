from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
import threading
import uuid
from collections.abc import AsyncIterable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

class StorageError(Exception): pass
class ObjectNotFoundError(StorageError): pass
class ObjectAlreadyExistsError(StorageError): pass
class StorageFullError(StorageError): pass

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
    METADATA_NAME = "metadata.json"
    CHUNK_PREFIX = "chunk-"

    def __init__(self, data_dir: Path, capacity_bytes: int, chunk_size_bytes: int = 16 * 1024**2) -> None:
        if capacity_bytes <= 0: raise ValueError("capacity_bytes must be greater than zero")
        if chunk_size_bytes <= 0: raise ValueError("chunk_size_bytes must be greater than zero")
        self.data_dir = data_dir.resolve()
        self.capacity_bytes = capacity_bytes
        self.chunk_size_bytes = chunk_size_bytes
        self._lock = threading.RLock()
        self._stream_lock = asyncio.Lock()
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def object_path(self, object_id: str, version_id: str) -> Path:
        self._validate_ids(object_id, version_id)
        return self.data_dir / "objects" / object_id / version_id

    def exists(self, object_id: str, version_id: str) -> bool:
        with self._lock:
            return (self.object_path(object_id, version_id) / self.METADATA_NAME).is_file()

    def object_size(self, object_id: str, version_id: str) -> int:
        with self._lock:
            return int(self._read_metadata(object_id, version_id)["size_bytes"])

    def write_bytes(self, object_id: str, version_id: str, data: bytes) -> int:
        if not isinstance(data, bytes): raise TypeError("data must be bytes")
        return self._write_iterable(object_id, version_id, (data,))

    async def write_stream(self, object_id: str, version_id: str, chunks: AsyncIterable[bytes]) -> int:
        async with self._stream_lock:
            self._validate_ids(object_id, version_id)
            with self._lock:
                self._ensure_new_object(object_id, version_id)
                staging_dir = self._create_staging_dir(object_id, version_id)
            size = chunk_index = chunk_written = 0
            chunk_handle = None
            chunk_digests: list[str] = []
            object_digest = hashlib.sha256()
            chunk_digest = hashlib.sha256()
            try:
                async for incoming in chunks:
                    if not isinstance(incoming, bytes): raise TypeError("request stream must yield bytes")
                    offset = 0
                    while offset < len(incoming):
                        if chunk_handle is None:
                            chunk_handle = (staging_dir / self._chunk_name(chunk_index)).open("xb")
                            chunk_written = 0
                            chunk_digest = hashlib.sha256()
                        piece = incoming[offset:offset + self.chunk_size_bytes - chunk_written]
                        with self._lock: self._ensure_capacity(len(piece))
                        chunk_handle.write(piece)
                        chunk_digest.update(piece)
                        object_digest.update(piece)
                        size += len(piece); chunk_written += len(piece); offset += len(piece)
                        if chunk_written == self.chunk_size_bytes:
                            chunk_handle.flush(); os.fsync(chunk_handle.fileno()); chunk_handle.close(); chunk_handle = None
                            chunk_digests.append(chunk_digest.hexdigest()); chunk_index += 1
                if chunk_handle is not None:
                    chunk_handle.flush(); os.fsync(chunk_handle.fileno()); chunk_handle.close(); chunk_handle = None
                    chunk_digests.append(chunk_digest.hexdigest()); chunk_index += 1
                metadata = {"object_id": object_id, "version_id": version_id, "size_bytes": size,
                            "chunk_size_bytes": self.chunk_size_bytes, "chunk_count": chunk_index,
                            "checksum": object_digest.hexdigest(), "chunk_checksums": chunk_digests}
                self._write_metadata(staging_dir, metadata)
                self._publish_staging(object_id, version_id, staging_dir)
                return size
            except Exception:
                if chunk_handle is not None:
                    try: chunk_handle.close()
                    except OSError: pass
                self._remove_tree(staging_dir)
                raise

    def read_bytes(self, object_id: str, version_id: str) -> bytes:
        return b"".join(self.iter_chunks(object_id, version_id))

    def iter_chunks(self, object_id: str, version_id: str) -> Iterator[bytes]:
        with self._lock:
            metadata = self._read_metadata(object_id, version_id)
            paths = [self.object_path(object_id, version_id) / self._chunk_name(i) for i in range(int(metadata["chunk_count"]))]
        for path in paths:
            with path.open("rb") as handle:
                while data := handle.read(self.chunk_size_bytes):
                    yield data

    def verify(self, object_id: str, version_id: str) -> VerificationResult:
        self._validate_ids(object_id, version_id)
        with self._lock:
            metadata = self._read_metadata(object_id, version_id)
            version_dir = self.object_path(object_id, version_id)
            count = int(metadata.get("chunk_count", -1))
            expected_chunks = metadata.get("chunk_checksums")
            expected_object = metadata.get("checksum")
            errors: list[str] = []; corrupt: list[int] = []
            if not isinstance(expected_chunks, list) or len(expected_chunks) != count:
                errors.append("invalid chunk_checksums metadata"); expected_chunks = []
            if not isinstance(expected_object, str):
                errors.append("invalid checksum metadata"); expected_object = None
            object_digest = hashlib.sha256()
            for i in range(max(count, 0)):
                path = version_dir / self._chunk_name(i)
                if not path.is_file():
                    corrupt.append(i); errors.append(f"missing chunk {i}"); continue
                digest = hashlib.sha256()
                try:
                    with path.open("rb") as handle:
                        while data := handle.read(self.chunk_size_bytes):
                            digest.update(data); object_digest.update(data)
                except OSError as exc:
                    corrupt.append(i); errors.append(f"unreadable chunk {i}: {exc}"); continue
                if i >= len(expected_chunks) or digest.hexdigest() != expected_chunks[i]:
                    corrupt.append(i); errors.append(f"checksum mismatch for chunk {i}")
            actual = object_digest.hexdigest()
            if expected_object is not None and actual != expected_object: errors.append("object checksum mismatch")
            return VerificationResult(object_id, version_id, not corrupt and not errors, actual, count, tuple(corrupt), tuple(errors))

    def delete(self, object_id: str, version_id: str) -> None:
        with self._lock:
            version_dir = self.object_path(object_id, version_id)
            if not (version_dir / self.METADATA_NAME).is_file(): raise ObjectNotFoundError(f"Object version not found: {object_id}/{version_id}")
            self._remove_tree(version_dir); self._fsync_directory(version_dir.parent)
            try:
                version_dir.parent.rmdir(); self._fsync_directory(version_dir.parent.parent)
            except OSError: pass

    def stats(self) -> StorageStats:
        with self._lock:
            usage = shutil.disk_usage(self.data_dir)
            used = sum(p.stat().st_size for p in self.data_dir.rglob("*") if p.is_file() and p.name.startswith(self.CHUNK_PREFIX))
            return StorageStats(self.capacity_bytes, used, min(max(self.capacity_bytes - used, 0), usage.free))

    def _write_iterable(self, object_id: str, version_id: str, chunks: Iterable[bytes]) -> int:
        self._validate_ids(object_id, version_id)
        with self._lock:
            self._ensure_new_object(object_id, version_id); staging_dir = self._create_staging_dir(object_id, version_id)
            size = chunk_index = 0; buffer = bytearray(); digests: list[str] = []; object_digest = hashlib.sha256()
            try:
                for incoming in chunks:
                    if not isinstance(incoming, bytes): raise TypeError("data chunks must be bytes")
                    offset = 0
                    while offset < len(incoming):
                        piece = incoming[offset:offset + self.chunk_size_bytes - len(buffer)]
                        buffer.extend(piece); object_digest.update(piece); size += len(piece); offset += len(piece)
                        if len(buffer) == self.chunk_size_bytes:
                            self._ensure_capacity(size); data = bytes(buffer); self._write_chunk(staging_dir, chunk_index, data)
                            digests.append(hashlib.sha256(data).hexdigest()); buffer.clear(); chunk_index += 1
                if buffer:
                    self._ensure_capacity(size); data = bytes(buffer); self._write_chunk(staging_dir, chunk_index, data)
                    digests.append(hashlib.sha256(data).hexdigest()); chunk_index += 1
                self._write_metadata(staging_dir, {"object_id": object_id, "version_id": version_id, "size_bytes": size,
                    "chunk_size_bytes": self.chunk_size_bytes, "chunk_count": chunk_index,
                    "checksum": object_digest.hexdigest(), "chunk_checksums": digests})
                self._publish_staging(object_id, version_id, staging_dir); return size
            except Exception:
                self._remove_tree(staging_dir); raise

    def _ensure_new_object(self, object_id: str, version_id: str) -> None:
        if self.exists(object_id, version_id): raise ObjectAlreadyExistsError(f"Object version already exists: {object_id}/{version_id}")

    def _create_staging_dir(self, object_id: str, version_id: str) -> Path:
        parent = self.object_path(object_id, version_id).parent; parent.mkdir(parents=True, exist_ok=True)
        path = parent / f".{version_id}.{uuid.uuid4().hex}.upload"; path.mkdir(); return path

    def _publish_staging(self, object_id: str, version_id: str, staging_dir: Path) -> None:
        final = self.object_path(object_id, version_id)
        if final.exists(): raise ObjectAlreadyExistsError(f"Object version already exists: {object_id}/{version_id}")
        os.replace(staging_dir, final); self._fsync_directory(final.parent)

    def _write_chunk(self, staging_dir: Path, index: int, data: bytes) -> None:
        with (staging_dir / self._chunk_name(index)).open("xb") as handle:
            handle.write(data); handle.flush(); os.fsync(handle.fileno())

    def _write_metadata(self, staging_dir: Path, metadata: dict[str, object]) -> None:
        target = staging_dir / self.METADATA_NAME; temp = staging_dir / f".{self.METADATA_NAME}.{uuid.uuid4().hex}.tmp"
        try:
            with temp.open("x", encoding="utf-8") as handle:
                json.dump(metadata, handle, separators=(",", ":"), sort_keys=True); handle.flush(); os.fsync(handle.fileno())
            os.replace(temp, target); self._fsync_directory(staging_dir)
        finally: temp.unlink(missing_ok=True)

    def _read_metadata(self, object_id: str, version_id: str) -> dict[str, object]:
        path = self.object_path(object_id, version_id) / self.METADATA_NAME
        if not path.is_file(): raise ObjectNotFoundError(f"Object version not found: {object_id}/{version_id}")
        try:
            with path.open("r", encoding="utf-8") as handle: return json.load(handle)
        except (OSError, json.JSONDecodeError) as exc: raise StorageError(f"Object metadata is unreadable: {object_id}/{version_id}") from exc

    def _ensure_capacity(self, required_bytes: int) -> None:
        if required_bytes < 0: raise ValueError("required_bytes must not be negative")
        stats = self.stats()
        if required_bytes > stats.free_bytes: raise StorageFullError(f"Insufficient storage capacity: required={required_bytes}, free={stats.free_bytes}")

    @classmethod
    def _chunk_name(cls, index: int) -> str: return f"{cls.CHUNK_PREFIX}{index:06d}"

    @classmethod
    def _validate_ids(cls, object_id: str, version_id: str) -> None:
        cls._validate_id(object_id, "object_id"); cls._validate_id(version_id, "version_id")

    @staticmethod
    def _validate_id(value: str, field_name: str) -> None:
        if not isinstance(value, str) or not value or value in {".", ".."} or "/" in value or "\\" in value or "\u0000" in value:
            raise ValueError(f"Invalid {field_name}")

    @staticmethod
    def _remove_tree(path: Path) -> None:
        if path.is_dir(): shutil.rmtree(path)
        elif path.exists(): path.unlink()

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        try: fd = os.open(directory, os.O_RDONLY)
        except OSError: return
        try: os.fsync(fd)
        except OSError: pass
        finally: os.close(fd)

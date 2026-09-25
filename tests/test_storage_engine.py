import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from storage.storage_engine import (
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    StorageEngine,
    StorageFullError,
)


def test_write_read_exists_delete(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    assert engine.write_bytes("obj-1", "ver-1", b"hello") == 5
    assert engine.exists("obj-1", "ver-1")
    assert engine.read_bytes("obj-1", "ver-1") == b"hello"
    engine.delete("obj-1", "ver-1")
    assert not engine.exists("obj-1", "ver-1")


def test_multiple_versions_share_object_directory(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"one")
    engine.write_bytes("obj-1", "ver-2", b"two")
    assert engine.read_bytes("obj-1", "ver-1") == b"one"
    assert engine.read_bytes("obj-1", "ver-2") == b"two"


def test_chunk_layout_and_metadata(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefghij")

    version_dir = tmp_path / "objects" / "obj-1" / "ver-1"
    assert (version_dir / "chunk-000000").read_bytes() == b"abcd"
    assert (version_dir / "chunk-000001").read_bytes() == b"efgh"
    assert (version_dir / "chunk-000002").read_bytes() == b"ij"
    assert (version_dir / "metadata.json").exists()


def test_duplicate_write_is_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"hello")
    with pytest.raises(ObjectAlreadyExistsError):
        engine.write_bytes("obj-1", "ver-1", b"again")


def test_concurrent_duplicate_writes_have_one_winner(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)

    def write():
        try:
            engine.write_bytes("obj-1", "ver-1", b"hello")
            return "created"
        except ObjectAlreadyExistsError:
            return "duplicate"

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: write(), range(8)))

    assert results.count("created") == 1
    assert results.count("duplicate") == 7
    assert engine.read_bytes("obj-1", "ver-1") == b"hello"


def test_failed_duplicate_write_leaves_no_temp_files(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"hello")

    with pytest.raises(ObjectAlreadyExistsError):
        engine.write_bytes("obj-1", "ver-1", b"again")

    version_parent = tmp_path / "objects" / "obj-1"
    assert list(version_parent.glob("*.upload")) == []


def test_missing_read_and_delete_are_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    for operation in (
        lambda: engine.read_bytes("obj-1", "ver-1"),
        lambda: engine.delete("obj-1", "ver-1"),
    ):
        with pytest.raises(ObjectNotFoundError):
            operation()


def test_path_traversal_and_invalid_ids_are_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    invalid_ids = [
        ("../escape", "ver-1"),
        ("obj-1", "../escape"),
        ("", "ver-1"),
        ("obj-1", ""),
        (".", "ver-1"),
        ("obj-1", ".."),
        ("obj/1", "ver-1"),
        ("obj-1", "ver\\1"),
        ("obj-1\x00", "ver-1"),
    ]

    for object_id, version_id in invalid_ids:
        with pytest.raises(ValueError):
            engine.object_path(object_id, version_id)


def test_capacity_is_enforced(tmp_path):
    engine = StorageEngine(tmp_path, 4, chunk_size_bytes=4)
    with pytest.raises(StorageFullError):
        engine.write_bytes("obj-1", "ver-1", b"12345")


def test_stats_track_written_and_deleted_bytes(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    assert engine.stats().used_bytes == 0

    engine.write_bytes("obj-1", "ver-1", b"12345")
    assert engine.stats().used_bytes == 5

    engine.delete("obj-1", "ver-1")
    assert engine.stats().used_bytes == 0


def test_stream_write_is_bounded_to_chunks(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)

    async def source():
        for piece in (b"ab", b"cdef", b"ghijk", b"l"):
            yield piece
            await asyncio.sleep(0)

    size = asyncio.run(engine.write_stream("obj-1", "ver-1", source()))
    assert size == 12
    assert engine.read_bytes("obj-1", "ver-1") == b"abcdefghijkl"
    assert len(list((tmp_path / "objects" / "obj-1" / "ver-1").glob("chunk-*"))) == 3


def test_checksums_are_stored_and_verify(tmp_path):
    import hashlib, json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    payload = b"abcdefghij"
    engine.write_bytes("obj-1", "ver-1", payload)
    metadata = json.loads((tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json").read_text())
    assert metadata["checksum"] == hashlib.sha256(payload).hexdigest()
    assert metadata["chunk_checksums"] == [
        hashlib.sha256(b"abcd").hexdigest(),
        hashlib.sha256(b"efgh").hexdigest(),
        hashlib.sha256(b"ij").hexdigest(),
    ]
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is True
    assert result.corrupt_chunks == ()

def test_verify_detects_corrupt_chunk(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefghij")
    path = tmp_path / "objects" / "obj-1" / "ver-1" / "chunk-000001"
    path.write_bytes(b"XXXX")
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert 1 in result.corrupt_chunks
    assert "object checksum mismatch" in result.errors

def test_verify_detects_missing_chunk(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefghij")
    (tmp_path / "objects" / "obj-1" / "ver-1" / "chunk-000001").unlink()
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert result.corrupt_chunks == (1,)

def test_empty_object_has_sha256_checksum(tmp_path):
    import hashlib
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"")
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is True
    assert result.checksum == hashlib.sha256(b"").hexdigest()


def test_verify_detects_corrupted_metadata_checksum(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefghij")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["checksum"] = "0" * 64
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert "object checksum mismatch" in result.errors

def test_verify_detects_corrupted_size_metadata(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefghij")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["size_bytes"] = 9
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert any("chunk 2 size mismatch" in e for e in result.errors)

def test_verify_detects_extra_chunk(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefgh")
    version_dir = tmp_path / "objects" / "obj-1" / "ver-1"
    (version_dir / "chunk-000002").write_bytes(b"ZZZZ")
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert 2 in result.corrupt_chunks
    assert "unexpected chunk chunk-000002" in result.errors

def test_verify_detects_chunk_size_metadata_corruption(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefgh")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["chunk_size_bytes"] = 3
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert any("chunk_count does not match size_bytes" in e for e in result.errors)


def test_async_capacity_is_reserved_and_released_on_failure(tmp_path):
    engine = StorageEngine(tmp_path, 8, chunk_size_bytes=4)

    engine.write_bytes("existing", "ver-1", b"1234")

    async def source():
        yield b"5678"
        yield b"9"

    with pytest.raises(StorageFullError):
        asyncio.run(engine.write_stream("new", "ver-1", source()))

    assert not engine.exists("new", "ver-1")
    assert engine.stats().used_bytes == 4
    assert engine.stats().free_bytes == 4

def test_failed_async_write_does_not_leave_staging_data(tmp_path):
    engine = StorageEngine(tmp_path, 8, chunk_size_bytes=4)

    async def source():
        yield b"1234"
        yield b"5678"
        yield b"9"

    with pytest.raises(StorageFullError):
        asyncio.run(engine.write_stream("obj-1", "ver-1", source()))

    assert list((tmp_path / "objects" / "obj-1").glob("*.upload")) == []
    assert engine.stats().used_bytes == 0

def test_stale_staging_upload_is_cleaned_on_engine_startup(tmp_path):
    staging = tmp_path / "objects" / "obj-1" / ".ver-1.deadbeef.upload"
    staging.mkdir(parents=True)
    (staging / "chunk-000000").write_bytes(b"stale")

    StorageEngine(tmp_path, 1024, chunk_size_bytes=4)

    assert not staging.exists()

def test_verify_handles_unreadable_metadata_as_invalid(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"data")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata_path.write_text("{not-json")

    result = engine.verify("obj-1", "ver-1")

    assert result.valid is False
    assert result.checksum is None
    assert any("metadata is unreadable" in error for error in result.errors)


def test_concurrent_async_duplicate_writes_have_one_winner(tmp_path):
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)

    async def source():
        yield b"abcd"

    async def run():
        async def write():
            try:
                await engine.write_stream("obj-1", "ver-1", source())
                return "created"
            except ObjectAlreadyExistsError:
                return "duplicate"

        return await asyncio.gather(*[write() for _ in range(8)])

    results = asyncio.run(run())

    assert results.count("created") == 1
    assert results.count("duplicate") == 7
    assert engine.read_bytes("obj-1", "ver-1") == b"abcd"


def test_verify_detects_invalid_chunk_checksum_metadata(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefgh")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["chunk_checksums"][0] = "bad"
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert any("invalid checksum metadata for chunk 0" in e for e in result.errors)

def test_verify_detects_chunk_checksum_list_length_mismatch(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"abcdefgh")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["chunk_checksums"].pop()
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert "chunk_checksums length does not match chunk_count" in result.errors

def test_verify_detects_object_metadata_identity_mismatch(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"data")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["object_id"] = "other-object"
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert "metadata object_id mismatch" in result.errors

def test_verify_detects_non_integer_layout_metadata(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"data")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["chunk_count"] = "1"
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert "invalid chunk_count" in result.errors

def test_verify_rejects_excessive_chunk_count_without_scanning_billions(tmp_path):
    import json
    engine = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    engine.write_bytes("obj-1", "ver-1", b"data")
    metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["chunk_count"] = StorageEngine.MAX_VERIFY_CHUNKS + 1
    metadata["chunk_checksums"] = []
    metadata_path.write_text(json.dumps(metadata))
    result = engine.verify("obj-1", "ver-1")
    assert result.valid is False
    assert "chunk_count exceeds verification limit" in result.errors

def test_verify_works_after_node_chunk_size_configuration_changes(tmp_path):
    writer = StorageEngine(tmp_path, 1024, chunk_size_bytes=4)
    writer.write_bytes("obj-1", "ver-1", b"abcdefghij")

    reader = StorageEngine(tmp_path, 1024, chunk_size_bytes=8)

    assert reader.read_bytes("obj-1", "ver-1") == b"abcdefghij"
    assert reader.verify("obj-1", "ver-1").valid is True

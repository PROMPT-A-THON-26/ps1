from concurrent.futures import ThreadPoolExecutor

import pytest

from storage.storage_engine import (
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    StorageEngine,
    StorageFullError,
)


def test_write_read_exists_delete(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    assert engine.write_bytes("obj-1", "ver-1", b"hello") == 5
    assert engine.exists("obj-1", "ver-1")
    assert engine.read_bytes("obj-1", "ver-1") == b"hello"
    engine.delete("obj-1", "ver-1")
    assert not engine.exists("obj-1", "ver-1")


def test_multiple_versions_share_object_directory(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    engine.write_bytes("obj-1", "ver-1", b"one")
    engine.write_bytes("obj-1", "ver-2", b"two")
    assert engine.read_bytes("obj-1", "ver-1") == b"one"
    assert engine.read_bytes("obj-1", "ver-2") == b"two"


def test_duplicate_write_is_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    engine.write_bytes("obj-1", "ver-1", b"hello")
    with pytest.raises(ObjectAlreadyExistsError):
        engine.write_bytes("obj-1", "ver-1", b"again")


def test_concurrent_duplicate_writes_have_one_winner(tmp_path):
    engine = StorageEngine(tmp_path, 1024)

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
    engine = StorageEngine(tmp_path, 1024)
    engine.write_bytes("obj-1", "ver-1", b"hello")

    with pytest.raises(ObjectAlreadyExistsError):
        engine.write_bytes("obj-1", "ver-1", b"again")

    version_dir = tmp_path / "objects" / "obj-1" / "ver-1"
    assert list(version_dir.glob("*.tmp")) == []


def test_missing_read_and_delete_are_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
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
    engine = StorageEngine(tmp_path, 4)
    with pytest.raises(StorageFullError):
        engine.write_bytes("obj-1", "ver-1", b"12345")


def test_stats_track_written_and_deleted_bytes(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    assert engine.stats().used_bytes == 0

    engine.write_bytes("obj-1", "ver-1", b"12345")
    assert engine.stats().used_bytes == 5

    engine.delete("obj-1", "ver-1")
    assert engine.stats().used_bytes == 0

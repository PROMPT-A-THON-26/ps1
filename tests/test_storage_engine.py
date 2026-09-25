from storage.storage_engine import ObjectAlreadyExistsError, ObjectNotFoundError, StorageEngine


def test_write_read_exists_delete(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    assert engine.write_bytes("obj-1", "ver-1", b"hello") == 5
    assert engine.exists("obj-1", "ver-1")
    assert engine.read_bytes("obj-1", "ver-1") == b"hello"

    engine.delete("obj-1", "ver-1")
    assert not engine.exists("obj-1", "ver-1")


def test_duplicate_write_is_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    engine.write_bytes("obj-1", "ver-1", b"hello")
    try:
        engine.write_bytes("obj-1", "ver-1", b"again")
    except ObjectAlreadyExistsError:
        pass
    else:
        raise AssertionError("duplicate object version must be rejected")


def test_missing_read_and_delete_are_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    for operation in (
        lambda: engine.read_bytes("obj-1", "ver-1"),
        lambda: engine.delete("obj-1", "ver-1"),
    ):
        try:
            operation()
        except ObjectNotFoundError:
            pass
        else:
            raise AssertionError("missing object version must be rejected")


def test_path_traversal_is_rejected(tmp_path):
    engine = StorageEngine(tmp_path, 1024)
    for object_id, version_id in [("../escape", "ver-1"), ("obj-1", "../escape")]:
        try:
            engine.object_path(object_id, version_id)
        except ValueError:
            pass
        else:
            raise AssertionError("path traversal must be rejected")

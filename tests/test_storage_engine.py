from __future__ import annotations

from unittest.mock import patch

import pytest

from storage.storage_engine import StorageEngine, StorageFullError


def test_usage_stats_are_cached_between_operations(tmp_path):
    engine = StorageEngine(
        tmp_path,
        capacity_bytes=10_000,
        chunk_size_bytes=4,
    )

    engine.write_bytes("object-a", "v1", b"abcdefgh")
    assert engine.stats().used_bytes == 8

    with patch.object(
        engine,
        "_scan_used_payload_bytes",
        side_effect=AssertionError("usage scan must not run after initialization"),
    ):
        engine.write_bytes("object-b", "v1", b"1234")
        assert engine.stats().used_bytes == 12

    engine.delete("object-a", "v1")
    assert engine.stats().used_bytes == 4


def test_capacity_reservation_rejects_an_oversized_stream_before_write(tmp_path):
    engine = StorageEngine(
        tmp_path,
        capacity_bytes=8,
        chunk_size_bytes=4,
    )

    with pytest.raises(StorageFullError):
        engine.write_bytes("too-large", "v1", b"123456789")

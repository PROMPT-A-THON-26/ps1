from uuid import UUID

import pytest
from sqlalchemy.exc import IntegrityError

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import ChecksumMismatch, InvalidState, ObjectAlreadyExists, VersionConflict
from metadata.manager import MetadataManager
from metadata.models import Replica


def test_object_version_commit_is_transactional(db_session):
    manager = MetadataManager(db_session)

    obj = manager.create_object("movie.mp4")
    assert isinstance(obj.object_id, UUID)
    assert obj.current_version_id is None

    version = manager.create_version(
        obj.object_id,
        size_bytes=1024,
        checksum="a" * 64,
    )
    assert version.version_number == 1
    assert version.state is VersionState.PREPARING

    committed = manager.commit_version(version.version_id)
    assert committed.state is VersionState.COMMITTED
    assert committed.committed_at is not None

    refreshed = manager.get_object("movie.mp4")
    assert refreshed is not None
    assert refreshed.current_version_id == version.version_id


def test_expected_version_rejects_stale_writer(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("notes.txt")

    v1 = manager.create_version(obj.object_id, size_bytes=1, checksum="b" * 64)
    manager.commit_version(v1.version_id)

    v2 = manager.create_version(
        obj.object_id,
        size_bytes=2,
        checksum="c" * 64,
        expected_current_version=1,
    )
    manager.commit_version(v2.version_id, expected_current_version=1)

    with pytest.raises(VersionConflict):
        manager.create_version(
            obj.object_id,
            size_bytes=3,
            checksum="d" * 64,
            expected_current_version=1,
        )


def test_replica_cannot_be_marked_healthy_without_verification(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("archive.zip")
    node = manager.register_node(
        node_id="node-01",
        address="http://vault-node-01:8001",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=512, checksum="e" * 64)
    replica = manager.create_replica(version.version_id, node.node_id)
    replica_id = replica.replica_id

    with pytest.raises(InvalidState):
        manager.set_replica_state(replica_id, ReplicaState.HEALTHY)

    manager.set_replica_state(replica_id, ReplicaState.COPYING)

    with pytest.raises(ChecksumMismatch):
        manager.mark_replica_healthy(
            replica_id,
            checksum="f" * 64,
            size_bytes=512,
        )

    healthy = manager.mark_replica_healthy(
        replica_id,
        checksum="e" * 64,
        size_bytes=512,
    )
    assert healthy.status is ReplicaState.HEALTHY
    assert healthy.last_verified_at is not None
    assert healthy.checksum == "e" * 64
    assert healthy.size_bytes == 512


def test_duplicate_object_name_is_rejected(db_session):
    manager = MetadataManager(db_session)
    manager.create_object("duplicate.txt")
    with pytest.raises(ObjectAlreadyExists):
        manager.create_object("duplicate.txt")


def test_direct_pending_to_healthy_transition_is_rejected(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("direct-health.bin")
    node = manager.register_node(
        node_id="node-direct",
        address="http://vault-node-direct:8001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=10, checksum="a" * 64)
    replica = manager.create_replica(version.version_id, node.node_id)
    with pytest.raises(InvalidState):
        manager.mark_replica_healthy(
            replica.replica_id,
            checksum="a" * 64,
            size_bytes=10,
        )


def test_non_hex_sha256_checksum_is_rejected(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("bad-checksum.bin")
    with pytest.raises(ValueError):
        manager.create_version(obj.object_id, size_bytes=1, checksum="g" * 64)


def test_duplicate_replica_for_same_version_and_node_is_rejected(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("photo.jpg")
    node = manager.register_node(
        node_id="node-02",
        address="http://vault-node-02:8001",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=100, checksum="1" * 64)

    first = manager.create_replica(version.version_id, node.node_id)
    assert first.status is ReplicaState.PENDING

    duplicate = Replica(version_id=version.version_id, node_id=node.node_id)
    db_session.add(duplicate)
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_node_heartbeat_updates_capacity_and_timestamp(db_session):
    manager = MetadataManager(db_session)
    node = manager.register_node(
        node_id="node-03",
        address="http://vault-node-03:8001",
        capacity_bytes=1000,
    )
    assert node.last_heartbeat_at is None

    updated = manager.update_node_heartbeat(
        node.node_id,
        capacity_bytes=2000,
        used_bytes=750,
        status=NodeState.HEALTHY,
    )
    assert updated.capacity_bytes == 2000
    assert updated.used_bytes == 750
    assert updated.free_bytes == 1250
    assert updated.status is NodeState.HEALTHY
    assert updated.last_heartbeat_at is not None

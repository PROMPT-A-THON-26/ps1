from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from common.constants import NodeState, ObjectState, ReplicaState, VersionState
from common.errors import (
    ChecksumMismatch,
    InvalidState,
    ObjectAlreadyExists,
    ObjectNotFound,
    VersionConflict,
)
from metadata.manager import MetadataManager
from metadata.models import Object, Replica, Version


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def test_object_version_commit_is_transactional(db_session):
    manager = MetadataManager(db_session)

    obj = manager.create_object("movie.mp4")
    assert obj.current_version_id is None

    version = manager.create_version(obj.object_id, size_bytes=1024, checksum=SHA_A)
    assert version.version_number == 1
    assert version.state is VersionState.PREPARING

    committed = manager.commit_version(version.version_id)
    assert committed.state is VersionState.COMMITTED
    assert committed.committed_at is not None

    refreshed = manager.get_object("movie.mp4")
    assert refreshed is not None
    assert refreshed.current_version_id == version.version_id


def test_object_name_is_normalized_for_create_and_read(db_session):
    manager = MetadataManager(db_session)
    created = manager.create_object("  notes.txt  ")

    assert created.name == "notes.txt"
    assert manager.get_object("  notes.txt  ").object_id == created.object_id


def test_first_expected_version_zero_is_supported(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("zero-base.txt")

    version = manager.create_version(
        obj.object_id,
        size_bytes=1,
        checksum=SHA_A,
        expected_current_version=0,
    )
    assert version.version_number == 1


def test_expected_version_rejects_stale_writer(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("notes.txt")

    v1 = manager.create_version(obj.object_id, size_bytes=1, checksum=SHA_A)
    manager.commit_version(v1.version_id)

    v2 = manager.create_version(
        obj.object_id,
        size_bytes=2,
        checksum=SHA_B,
        expected_current_version=1,
    )
    manager.commit_version(v2.version_id, expected_current_version=1)

    with pytest.raises(VersionConflict):
        manager.create_version(
            obj.object_id,
            size_bytes=3,
            checksum=SHA_C,
            expected_current_version=1,
        )

    current = manager.get_object("notes.txt")
    assert current.current_version_id == v2.version_id


def test_commit_rejects_non_preparing_version(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("versions.txt")

    v1 = manager.create_version(obj.object_id, size_bytes=1, checksum=SHA_A)
    manager.commit_version(v1.version_id)

    with pytest.raises(InvalidState):
        manager.commit_version(v1.version_id)


def test_replica_cannot_be_marked_healthy_without_verification(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("archive.zip")
    node = manager.register_node(
        node_id="node-01",
        address="http://vault-node-01:8001/",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    assert node.address == "http://vault-node-01:8001"

    version = manager.create_version(obj.object_id, size_bytes=512, checksum=SHA_A)
    replica = manager.create_replica(version.version_id, node.node_id)

    with pytest.raises(InvalidState):
        manager.set_replica_state(replica.replica_id, ReplicaState.HEALTHY)

    manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)

    with pytest.raises(ChecksumMismatch):
        manager.mark_replica_healthy(
            replica.replica_id, checksum=SHA_B, size_bytes=512
        )

    with pytest.raises(InvalidState):
        manager.mark_replica_healthy(
            replica.replica_id, checksum=SHA_A, size_bytes=511
        )

    healthy = manager.mark_replica_healthy(
        replica.replica_id,
        checksum=SHA_A,
        size_bytes=512,
    )
    assert healthy.status is ReplicaState.HEALTHY
    assert healthy.last_verified_at is not None
    assert healthy.checksum == SHA_A
    assert healthy.size_bytes == 512


def test_replica_state_machine_enforces_canonical_transitions(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("state.bin")
    node = manager.register_node(
        node_id="node-state",
        address="http://node-state:9001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=10, checksum=SHA_A)
    replica = manager.create_replica(version.version_id, node.node_id)

    manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)
    with pytest.raises(InvalidState):
        manager.set_replica_state(replica.replica_id, ReplicaState.HEALTHY)

    manager.mark_replica_healthy(
        replica.replica_id,
        checksum=SHA_A,
        size_bytes=10,
    )
    manager.set_replica_state(replica.replica_id, ReplicaState.CORRUPTED)
    manager.set_replica_state(replica.replica_id, ReplicaState.REPAIRING)
    manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)
    manager.mark_replica_healthy(
        replica.replica_id,
        checksum=SHA_A,
        size_bytes=10,
    )

    with pytest.raises(InvalidState):
        manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)


def test_direct_pending_to_healthy_transition_is_rejected(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("direct-health.bin")
    node = manager.register_node(
        node_id="node-direct",
        address="http://vault-node-direct:8001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=10, checksum=SHA_A)
    replica = manager.create_replica(version.version_id, node.node_id)

    with pytest.raises(InvalidState):
        manager.mark_replica_healthy(
            replica.replica_id,
            checksum=SHA_A,
            size_bytes=10,
        )


def test_failed_replica_can_reenter_repair_only(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("failed.bin")
    node = manager.register_node(
        node_id="node-failed",
        address="http://node-failed:9001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=10, checksum=SHA_A)
    replica = manager.create_replica(version.version_id, node.node_id)
    manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)
    manager.set_replica_state(replica.replica_id, ReplicaState.FAILED)

    with pytest.raises(InvalidState):
        manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)


def test_duplicate_object_name_is_rejected(db_session):
    manager = MetadataManager(db_session)
    manager.create_object("duplicate.txt")
    with pytest.raises(ObjectAlreadyExists):
        manager.create_object("duplicate.txt")


def test_non_hex_sha256_checksum_is_rejected(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("bad-checksum.bin")

    with pytest.raises(ValueError):
        manager.create_version(obj.object_id, size_bytes=1, checksum="g" * 64)

    with pytest.raises(ValueError):
        manager.create_version(obj.object_id, size_bytes=-1, checksum=SHA_A)


def test_uppercase_checksum_is_normalized(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("uppercase.txt")
    version = manager.create_version(
        obj.object_id, size_bytes=1, checksum=SHA_A.upper()
    )
    assert version.checksum == SHA_A


def test_duplicate_replica_is_rejected_before_database_error(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("photo.jpg")
    node = manager.register_node(
        node_id="node-02",
        address="http://vault-node-02:8001",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=100, checksum=SHA_A)

    manager.create_replica(version.version_id, node.node_id)

    with pytest.raises(InvalidState):
        manager.create_replica(version.version_id, node.node_id)


def test_database_unique_constraint_still_protects_direct_inserts(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("photo-direct.jpg")
    node = manager.register_node(
        node_id="node-direct-2",
        address="http://vault-node-direct-2:8001",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=100, checksum=SHA_A)

    manager.create_replica(version.version_id, node.node_id)
    duplicate = Replica(version_id=version.version_id, node_id=node.node_id)
    db_session.add(duplicate)

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_current_version_foreign_key_is_enforced(db_session):
    obj = Object(name="broken-current", state=ObjectState.ACTIVE)
    db_session.add(obj)
    db_session.commit()

    obj.current_version_id = uuid4()
    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_deleted_object_cannot_receive_new_versions(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("deleted.txt")
    obj.state = ObjectState.DELETED
    db_session.commit()

    with pytest.raises(InvalidState):
        manager.create_version(obj.object_id, size_bytes=1, checksum=SHA_A)


def test_node_registration_is_idempotent_by_address(db_session):
    manager = MetadataManager(db_session)
    first = manager.register_node(
        node_id="node-idempotent",
        address="http://node-idempotent:9001/",
        capacity_bytes=1000,
    )
    second = manager.register_node(
        node_id="node-idempotent",
        address="http://node-idempotent:9001",
        capacity_bytes=2000,
    )

    assert second.node_id == first.node_id
    assert second.capacity_bytes == 1000


def test_conflicting_node_id_for_same_address_is_rejected(db_session):
    manager = MetadataManager(db_session)
    manager.register_node(
        node_id="node-original",
        address="http://same-node:9001",
        capacity_bytes=1000,
    )

    with pytest.raises(ObjectAlreadyExists):
        manager.register_node(
            node_id="node-other",
            address="http://same-node:9001/",
            capacity_bytes=1000,
        )


def test_node_heartbeat_updates_capacity_and_timestamp(db_session):
    manager = MetadataManager(db_session)
    node = manager.register_node(
        node_id="node-03",
        address="http://vault-node-03:8001",
        capacity_bytes=1000,
    )
    assert node.last_heartbeat_at is None

    t2 = datetime.now(timezone.utc)
    updated = manager.update_node_heartbeat(
        node.node_id,
        capacity_bytes=2000,
        used_bytes=750,
        status=NodeState.HEALTHY,
        heartbeat_at=t2,
    )
    assert updated.capacity_bytes == 2000
    assert updated.used_bytes == 750
    assert updated.free_bytes == 1250
    assert updated.status is NodeState.HEALTHY
    assert updated.last_heartbeat_at is not None

    stale = manager.update_node_heartbeat(
        node.node_id,
        capacity_bytes=3000,
        used_bytes=100,
        heartbeat_at=t2 - timedelta(seconds=5),
    )
    assert stale.last_heartbeat_at == t2


def test_heartbeat_rejects_used_above_capacity(db_session):
    manager = MetadataManager(db_session)
    node = manager.register_node(
        node_id="node-capacity",
        address="http://node-capacity:9001",
        capacity_bytes=100,
    )

    with pytest.raises(ValueError):
        manager.update_node_heartbeat(node.node_id, used_bytes=101)


def test_missing_objects_and_nodes_raise_domain_errors(db_session):
    manager = MetadataManager(db_session)

    with pytest.raises(ObjectNotFound):
        manager.get_object_or_raise("missing")

    with pytest.raises(ObjectNotFound):
        manager.update_node_heartbeat("missing-node")

    with pytest.raises(ObjectNotFound):
        manager.create_replica(uuid4(), "missing-node")


def test_metadata_relationships_round_trip(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("relations.bin")
    node = manager.register_node(
        node_id="node-rel",
        address="http://node-rel:9001",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    version = manager.create_version(obj.object_id, size_bytes=42, checksum=SHA_A)
    replica = manager.create_replica(version.version_id, node.node_id)

    fetched_version = db_session.scalar(
        select(Version).where(Version.version_id == version.version_id)
    )
    fetched_replica = db_session.scalar(
        select(Replica).where(Replica.replica_id == replica.replica_id)
    )

    assert fetched_version.object.object_id == obj.object_id
    assert fetched_replica.version.version_id == version.version_id
    assert fetched_replica.node.node_id == node.node_id

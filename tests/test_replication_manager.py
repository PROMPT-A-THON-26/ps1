from __future__ import annotations

import pytest

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import VaultError
from metadata.manager import MetadataManager
from metadata.models import Replica
from replication import ReplicationManager, ReplicationPolicy
from replication.node_client import (
    StoredObject,
    StorageNodeUnavailableError,
    VerifiedObject,
    StorageObjectAlreadyExistsError,
)

SHA_A = "a" * 64


class FakeClient:
    calls: list[tuple[str, str]] = []
    behaviors: dict[str, str] = {}

    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        self.calls.append(("put", self.node_id))
        behavior = self.behaviors.get(self.node_id)
        if behavior == "exists":
            raise StorageObjectAlreadyExistsError("already exists")
        if behavior == "fail":
            raise StorageNodeUnavailableError("unavailable")
        return StoredObject(object_id, version_id, 5)

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        self.calls.append(("verify", self.node_id))
        behavior = self.behaviors.get(self.node_id)
        checksum = "b" * 64 if behavior == "corrupt" else SHA_A
        return VerifiedObject(object_id, version_id, 5, checksum, True, True)

    async def aclose(self) -> None:
        return None


def seed(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("replicated.txt")
    version = manager.create_version(obj.object_id, size_bytes=5, checksum=SHA_A)
    for index, node_id in enumerate(("node-a", "node-b", "node-c")):
        manager.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=1000 + index,
            status=NodeState.HEALTHY,
        )
    db_session.commit()
    FakeClient.calls = []
    FakeClient.behaviors = {}
    return version


@pytest.mark.asyncio
async def test_replication_writes_verifies_and_commits(db_session):
    version = seed(db_session)
    result = await ReplicationManager(
        db_session, client_factory=FakeClient
    ).replicate_version(version.version_id, payload_factory=lambda: b"hello")

    assert result.committed is True
    assert result.healthy_count == 3
    assert version.state is VersionState.COMMITTED
    assert db_session.query(Replica).filter_by(
        version_id=version.version_id, status=ReplicaState.HEALTHY
    ).count() == 3


@pytest.mark.asyncio
async def test_existing_object_is_verified_before_health_promotion(db_session):
    version = seed(db_session)
    FakeClient.behaviors = {"node-c": "exists"}

    result = await ReplicationManager(
        db_session, client_factory=FakeClient
    ).replicate_version(version.version_id, payload_factory=lambda: b"hello")

    assert result.healthy_count == 3
    assert ("put", "node-c") in FakeClient.calls
    assert ("verify", "node-c") in FakeClient.calls


@pytest.mark.asyncio
async def test_failed_replica_is_not_healthy_when_quorum_is_met(db_session):
    version = seed(db_session)
    FakeClient.behaviors = {"node-c": "fail"}

    result = await ReplicationManager(
        db_session,
        client_factory=FakeClient,
        policy=ReplicationPolicy(factor=3, write_quorum=2, read_quorum=1),
    ).replicate_version(version.version_id, payload_factory=lambda: b"hello")

    assert result.committed is True
    assert set(result.healthy_node_ids) == {"node-a", "node-b"}
    assert result.failed_node_ids == ("node-c",)
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id, node_id="node-c"
    ).one()
    assert replica.status is ReplicaState.FAILED
    assert version.state is VersionState.COMMITTED


@pytest.mark.asyncio
async def test_quorum_failure_does_not_commit(db_session):
    version = seed(db_session)
    FakeClient.behaviors = {"node-a": "fail", "node-b": "fail", "node-c": "fail"}

    with pytest.raises(VaultError) as exc:
        await ReplicationManager(
            db_session, client_factory=FakeClient
        ).replicate_version(version.version_id, payload_factory=lambda: b"hello")

    assert exc.value.status_code == 503
    assert version.state is VersionState.PREPARING


@pytest.mark.asyncio
async def test_checksum_mismatch_cannot_promote_replica(db_session):
    version = seed(db_session)
    FakeClient.behaviors = {"node-c": "corrupt"}

    result = await ReplicationManager(
        db_session,
        client_factory=FakeClient,
        policy=ReplicationPolicy(factor=3, write_quorum=2, read_quorum=1),
    ).replicate_version(version.version_id, payload_factory=lambda: b"hello")

    assert result.committed is True
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id, node_id="node-c"
    ).one()
    assert replica.status is ReplicaState.FAILED


def test_replication_policy_validates_quorums():
    with pytest.raises(ValueError):
        ReplicationPolicy(factor=2, write_quorum=3)
    with pytest.raises(ValueError):
        ReplicationPolicy(factor=2, read_quorum=3)

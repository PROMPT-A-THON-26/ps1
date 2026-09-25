from __future__ import annotations

from contextlib import asynccontextmanager
from hashlib import sha256

import pytest

from common.constants import JobStatus, NodeState, ReplicaState
from common.errors import VaultError
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica
from repair import RepairManager


DATA = b"vault-repair-payload"
CHECKSUM = sha256(DATA).hexdigest()


class FakeClient:
    storage: dict[tuple[str, str, str], bytes] = {}
    fail_nodes: set[str] = set()

    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        from replication.node_client import VerifiedObject
        if self.node_id in self.fail_nodes:
            from replication.node_client import StorageNodeUnavailableError
            raise StorageNodeUnavailableError("unavailable")
        body = self.storage[(self.node_id, object_id, version_id)]
        return VerifiedObject(
            object_id,
            version_id,
            len(body),
            sha256(body).hexdigest(),
            True,
            True,
        )

    @asynccontextmanager
    async def stream_object(self, object_id: str, version_id: str, **kwargs):
        from replication.node_client import StorageNodeUnavailableError
        if self.node_id in self.fail_nodes:
            raise StorageNodeUnavailableError("unavailable")

        class Response:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def aiter_bytes(self):
                return _aiter(self.body)

        yield Response(self.storage[(self.node_id, object_id, version_id)])

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        if self.node_id in self.fail_nodes:
            from replication.node_client import StorageNodeUnavailableError
            raise StorageNodeUnavailableError("unavailable")
        parts = []
        async for chunk in data:
            parts.append(bytes(chunk))
        body = b"".join(parts)
        self.storage[(self.node_id, object_id, version_id)] = body
        from replication.node_client import StoredObject
        return StoredObject(object_id, version_id, len(body))

    async def aclose(self):
        return None


async def _aiter(body: bytes):
    yield body


def seed(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("repair.txt")
    version = manager.create_version(obj.object_id, size_bytes=len(DATA), checksum=CHECKSUM)
    manager.register_node(node_id="node-a", address="http://node-a:9001", capacity_bytes=1000, status=NodeState.HEALTHY)
    manager.register_node(node_id="node-b", address="http://node-b:9001", capacity_bytes=900, status=NodeState.HEALTHY)
    manager.register_node(node_id="node-c", address="http://node-c:9001", capacity_bytes=800, status=NodeState.HEALTHY)
    manager.register_node(node_id="node-d", address="http://node-d:9001", capacity_bytes=700, status=NodeState.HEALTHY)
    manager.create_replica(version.version_id, "node-a")
    manager.set_replica_state(
        db_session.query(Replica).filter_by(version_id=version.version_id, node_id="node-a").one().replica_id,
        ReplicaState.COPYING,
    )
    manager.mark_replica_healthy(
        db_session.query(Replica).filter_by(version_id=version.version_id, node_id="node-a").one().replica_id,
        checksum=CHECKSUM,
        size_bytes=len(DATA),
    )
    FakeClient.storage = {
        ("node-a", str(obj.object_id), str(version.version_id)): DATA
    }
    FakeClient.fail_nodes = set()
    db_session.commit()
    return obj, version


@pytest.mark.asyncio
async def test_repair_creates_durable_job_and_restores_replication(db_session):
    obj, version = seed(db_session)
    manager = RepairManager(db_session, client_factory=FakeClient)

    results = await manager.repair_version_until_healthy(
        version.version_id,
        replication_factor=3,
    )

    assert len(results) == 2
    assert all(result.status is JobStatus.SUCCEEDED for result in results)
    healthy = db_session.query(Replica).filter_by(
        version_id=version.version_id, status=ReplicaState.HEALTHY
    ).all()
    assert {r.node_id for r in healthy} == {"node-a", "node-b", "node-c"}
    assert db_session.query(RepairJob).count() == 2


@pytest.mark.asyncio
async def test_corrupted_replica_is_repaired_in_place(db_session):
    obj, version = seed(db_session)
    metadata = MetadataManager(db_session)
    replica = metadata.create_replica(version.version_id, "node-d")
    metadata.set_replica_state(replica.replica_id, ReplicaState.REPAIRING)
    metadata.set_replica_state(replica.replica_id, ReplicaState.COPYING)
    replica.status = ReplicaState.CORRUPTED
    db_session.commit()

    FakeClient.storage[("node-d", str(obj.object_id), str(version.version_id))] = b"bad"
    manager = RepairManager(db_session, client_factory=FakeClient)
    job = manager.schedule_for_version(
        version.version_id,
        replication_factor=2,
        reason="corruption",
        preferred_replica=replica,
    )
    result = await manager.run_job(job.repair_id)

    assert result.status is JobStatus.SUCCEEDED
    assert replica.status is ReplicaState.HEALTHY
    assert FakeClient.storage[("node-d", str(obj.object_id), str(version.version_id))] == DATA


@pytest.mark.asyncio
async def test_repair_requires_verified_healthy_source(db_session):
    _, version = seed(db_session)
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id, node_id="node-a"
    ).one()
    replica.status = ReplicaState.CORRUPTED
    replica.checksum = CHECKSUM
    db_session.commit()

    with pytest.raises(VaultError) as exc:
        RepairManager(db_session, client_factory=FakeClient).schedule_for_version(
            version.version_id,
            replication_factor=2,
        )
    assert exc.value.status_code == 503


@pytest.mark.asyncio
async def test_failed_repair_is_persisted_for_retry(db_session):
    _, version = seed(db_session)
    FakeClient.fail_nodes = {"node-a"}
    manager = RepairManager(db_session, client_factory=FakeClient, max_attempts=2)
    job = manager.schedule_for_version(version.version_id, replication_factor=2)

    with pytest.raises(Exception):
        await manager.run_job(job.repair_id)

    refreshed = db_session.query(RepairJob).one()
    assert refreshed.attempts == 1
    assert refreshed.status is JobStatus.PENDING
    assert refreshed.last_error

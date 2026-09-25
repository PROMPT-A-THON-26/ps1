from __future__ import annotations

from contextlib import asynccontextmanager
from hashlib import sha256

import pytest

from common.constants import JobStatus, NodeState, ReplicaState, VersionState
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica
from repair import RepairManager
from integrity import IntegrityManager


DATA = b"vault-integrity-payload"
CHECKSUM = sha256(DATA).hexdigest()


class FakeClient:
    storage: dict[tuple[str, str, str], bytes] = {}
    fail_nodes: set[str] = set()

    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        from replication.node_client import VerifiedObject, StorageNodeUnavailableError
        if self.node_id in self.fail_nodes:
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
        class Response:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def aiter_bytes(self):
                return _aiter(self.body)

        yield Response(self.storage[(self.node_id, object_id, version_id)])

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        parts = []
        async for chunk in data:
            parts.append(bytes(chunk))
        body = b"".join(parts)
        self.storage[(self.node_id, object_id, version_id)] = body
        from replication.node_client import StoredObject
        return StoredObject(object_id, version_id, len(body))

    async def delete_object(self, object_id: str, version_id: str, **kwargs):
        self.storage.pop((self.node_id, object_id, version_id), None)

    async def aclose(self):
        return None


async def _aiter(body: bytes):
    yield body


def _seed(db_session, *, replicas=("node-a",)):
    manager = MetadataManager(db_session)
    obj = manager.create_object("integrity.txt")
    version = manager.create_version(obj.object_id, size_bytes=len(DATA), checksum=CHECKSUM)
    for node_id, capacity in (("node-a", 1000), ("node-b", 900), ("node-c", 800)):
        manager.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=capacity,
            status=NodeState.HEALTHY,
        )

    for node_id in replicas:
        replica = manager.create_replica(version.version_id, node_id)
        manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        manager.mark_replica_healthy(
            replica.replica_id,
            checksum=CHECKSUM,
            size_bytes=len(DATA),
        )

    manager.commit_version(version.version_id)
    FakeClient.storage = {
        (node_id, str(obj.object_id), str(version.version_id)): DATA
        for node_id in replicas
    }
    FakeClient.fail_nodes = set()
    db_session.commit()
    return obj, version


@pytest.mark.asyncio
async def test_healthy_replica_verification_refreshes_integrity_metadata(db_session):
    _, version = _seed(db_session)
    replica = db_session.query(Replica).filter_by(version_id=version.version_id).one()
    replica.checksum = None
    replica.size_bytes = None
    db_session.commit()

    result = await IntegrityManager(db_session, client_factory=FakeClient).verify_replica(
        replica.replica_id
    )

    assert result.checked is True
    assert result.corrupted is False
    db_session.refresh(replica)
    assert replica.status is ReplicaState.HEALTHY
    assert replica.checksum == CHECKSUM
    assert replica.size_bytes == len(DATA)
    assert replica.last_verified_at is not None


@pytest.mark.asyncio
async def test_checksum_mismatch_marks_corrupt_and_enqueues_verified_repair(db_session):
    obj, version = _seed(db_session, replicas=("node-a", "node-b", "node-c"))
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()
    FakeClient.storage[("node-b", str(obj.object_id), str(version.version_id))] = b"corrupt"

    result = await IntegrityManager(
        db_session,
        client_factory=FakeClient,
        replication_factor=3,
    ).verify_replica(replica.replica_id)

    assert result.checked is True
    assert result.corrupted is True
    assert result.repair_id is not None
    db_session.refresh(replica)
    assert replica.status is ReplicaState.CORRUPTED

    job = db_session.query(RepairJob).one()
    assert job.status is JobStatus.PENDING
    assert job.source_node_id in {"node-a", "node-c"}
    assert job.target_node_id == "node-b"

    repair = RepairManager(db_session, client_factory=FakeClient)
    repaired = await repair.run_job(job.repair_id)
    assert repaired.status is JobStatus.SUCCEEDED
    db_session.refresh(replica)
    assert replica.status is ReplicaState.HEALTHY
    assert FakeClient.storage[("node-b", str(obj.object_id), str(version.version_id))] == DATA


@pytest.mark.asyncio
async def test_unhealthy_node_is_skipped_and_not_marked_corrupt(db_session):
    _, version = _seed(db_session)
    replica = db_session.query(Replica).filter_by(version_id=version.version_id).one()
    metadata = MetadataManager(db_session)
    metadata.transition_node_state("node-a", NodeState.SUSPECT)

    result = await IntegrityManager(db_session, client_factory=FakeClient).verify_replica(
        replica.replica_id
    )

    assert result.checked is False
    assert result.corrupted is False
    db_session.refresh(replica)
    assert replica.status is ReplicaState.HEALTHY
    assert db_session.query(RepairJob).count() == 0

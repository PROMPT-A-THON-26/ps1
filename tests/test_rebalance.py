from __future__ import annotations

from contextlib import asynccontextmanager
from hashlib import sha256

import pytest

from common.constants import JobStatus, NodeState, ReplicaState
from common.errors import InvalidState
from metadata.manager import MetadataManager
from metadata.models import RebalanceJob, Replica, StorageNode
from rebalance import RebalanceManager
from replication.node_client import StorageNodeIntegrityError, StorageNodeUnavailableError


DATA = b"rebalance-payload"
CHECKSUM = sha256(DATA).hexdigest()


class FakeClient:
    storage: dict[tuple[str, str, str], bytes] = {}
    events: list[tuple[str, str]] = []
    fail_nodes: set[str] = set()

    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        from replication.node_client import VerifiedObject

        self.events.append(("verify", self.node_id))
        if self.node_id in self.fail_nodes:
            raise StorageNodeUnavailableError("node unavailable")
        body = self.storage.get((self.node_id, object_id, version_id))
        if body is None:
            raise StorageNodeIntegrityError("object missing")
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
        if self.node_id in self.fail_nodes:
            raise StorageNodeUnavailableError("node unavailable")

        class Response:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def aiter_bytes(self):
                return _aiter(self.body)

        yield Response(self.storage[(self.node_id, object_id, version_id)])

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        if self.node_id in self.fail_nodes:
            raise StorageNodeUnavailableError("node unavailable")
        parts = []
        async for chunk in data:
            parts.append(bytes(chunk))
        body = b"".join(parts)
        self.events.append(("put", self.node_id))
        self.storage[(self.node_id, object_id, version_id)] = body
        from replication.node_client import StoredObject
        return StoredObject(object_id, version_id, len(body))

    async def delete_object(self, object_id: str, version_id: str, **kwargs):
        if self.node_id in self.fail_nodes:
            raise StorageNodeUnavailableError("node unavailable")
        self.events.append(("delete", self.node_id))
        self.storage.pop((self.node_id, object_id, version_id), None)

    async def aclose(self):
        return None


async def _aiter(body: bytes):
    yield body


def _seed(db_session):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("rebalance.bin")
    version = metadata.create_version(
        obj.object_id,
        size_bytes=len(DATA),
        checksum=CHECKSUM,
    )

    for node_id, capacity in (
        ("node-a", 1000),
        ("node-b", 900),
        ("node-c", 800),
        ("node-d", 700),
    ):
        metadata.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=capacity,
            status=NodeState.HEALTHY,
        )

    for node_id in ("node-a", "node-b", "node-c"):
        replica = metadata.create_replica(version.version_id, node_id)
        metadata.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        metadata.mark_replica_healthy(
            replica.replica_id,
            checksum=CHECKSUM,
            size_bytes=len(DATA),
        )

    metadata.commit_version(version.version_id)
    FakeClient.storage = {
        (node_id, str(obj.object_id), str(version.version_id)): DATA
        for node_id in ("node-a", "node-b", "node-c")
    }
    FakeClient.events = []
    FakeClient.fail_nodes = set()
    db_session.commit()
    return obj, version


@pytest.mark.asyncio
async def test_migrate_replica_copies_verifies_then_deletes_source(db_session):
    obj, version = _seed(db_session)
    manager = RebalanceManager(db_session, client_factory=FakeClient, replication_factor=3)

    result = await manager.migrate_replica(
        version.version_id,
        source_node_id="node-a",
        target_node_id="node-d",
    )

    assert result.status is JobStatus.SUCCEEDED
    assert result.source_removed is True
    replicas = db_session.query(Replica).filter_by(version_id=version.version_id).all()
    assert {replica.node_id for replica in replicas} == {"node-b", "node-c", "node-d"}
    assert all(replica.status is ReplicaState.HEALTHY for replica in replicas)
    assert FakeClient.storage[("node-d", str(obj.object_id), str(version.version_id))] == DATA
    assert ("delete", "node-a") in FakeClient.events
    source_delete_index = FakeClient.events.index(("delete", "node-a"))
    target_verify_indexes = [
        index for index, event in enumerate(FakeClient.events)
        if event == ("verify", "node-d")
    ]
    assert target_verify_indexes
    assert max(target_verify_indexes) < source_delete_index


@pytest.mark.asyncio
async def test_drain_node_removes_node_only_after_all_replicas_move(db_session):
    _, version = _seed(db_session)
    manager = RebalanceManager(db_session, client_factory=FakeClient, replication_factor=3)

    results = await manager.drain_node("node-a")

    assert len(results) == 1
    assert results[0].source_node_id == "node-a"
    assert results[0].target_node_id == "node-d"
    node = db_session.get(StorageNode, "node-a")
    assert node.status is NodeState.REMOVED
    assert db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-a",
    ).count() == 0
    assert {replica.node_id for replica in db_session.query(Replica).filter_by(
        version_id=version.version_id
    ).all()} == {"node-b", "node-c", "node-d"}


@pytest.mark.asyncio
async def test_failed_rebalance_keeps_source_replica_and_persists_job(db_session):
    obj, version = _seed(db_session)
    FakeClient.fail_nodes = {"node-d"}
    manager = RebalanceManager(
        db_session,
        client_factory=FakeClient,
        replication_factor=3,
        max_attempts=2,
    )
    job = manager.create_job(
        version.version_id,
        source_node_id="node-a",
        target_node_id="node-d",
    )

    with pytest.raises(StorageNodeUnavailableError):
        await manager.run_job(job.rebalance_id)

    refreshed = db_session.get(RebalanceJob, job.rebalance_id)
    assert refreshed.status is JobStatus.PENDING
    assert refreshed.attempts == 1
    assert refreshed.last_error
    assert db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-a",
        status=ReplicaState.HEALTHY,
    ).count() == 1
    assert FakeClient.storage[("node-a", str(obj.object_id), str(version.version_id))] == DATA


def test_drain_blocks_on_nonhealthy_replica(db_session):
    _, version = _seed(db_session)
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-a",
    ).one()
    replica.status = ReplicaState.CORRUPTED
    db_session.commit()

    with pytest.raises(InvalidState):
        import asyncio
        asyncio.run(
            RebalanceManager(db_session, client_factory=FakeClient).drain_node("node-a")
        )

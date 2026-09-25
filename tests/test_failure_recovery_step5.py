"""Step 5 failure-detection and recovery scenarios on the reliability control plane."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import AsyncIterator

import pytest
from sqlalchemy import select

from common.constants import JobStatus, NodeState, ReplicaState
from health.failure_detector import FailureDetector, FailureDetectorConfig
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, StorageNode, Version
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import (
    NodeHealth,
    NodeStats,
    StorageNodeUnavailableError,
    VerifiedObject,
)
from repair.worker import RepairWorker


PAYLOAD = b"vault-step5-failure-recovery"
CHECKSUM = sha256(PAYLOAD).hexdigest()


class InMemoryNode:
    storage: dict[tuple[str, str], bytes] = {}
    unavailable: set[str] = set()

    def __init__(self, node_id: str):
        self.node_id = node_id

    async def health(self) -> NodeHealth:
        if self.node_id in self.unavailable:
            raise StorageNodeUnavailableError(f"{self.node_id} unavailable")
        return NodeHealth(status="healthy", node_id=self.node_id)

    async def stats(self) -> NodeStats:
        if self.node_id in self.unavailable:
            raise StorageNodeUnavailableError(f"{self.node_id} unavailable")
        used = sum(
            len(value)
            for (node_id, _), value in self.storage.items()
            if node_id == self.node_id
        )
        return NodeStats(
            node_id=self.node_id,
            capacity_bytes=1024,
            used_bytes=used,
            free_bytes=1024 - used,
        )

    async def verify_object(self, object_id: str, version_id: str, **kwargs) -> VerifiedObject:
        if self.node_id in self.unavailable:
            raise StorageNodeUnavailableError(f"{self.node_id} unavailable")
        body = self.storage.get((self.node_id, version_id))
        if body is None:
            raise StorageNodeUnavailableError("replica missing")
        return VerifiedObject(
            object_id=object_id,
            version_id=version_id,
            size_bytes=len(body),
            checksum=sha256(body).hexdigest(),
            verified=True,
            valid=True,
        )

    @asynccontextmanager
    async def stream_object(
        self,
        object_id: str,
        version_id: str,
        **kwargs,
    ) -> AsyncIterator[object]:
        if self.node_id in self.unavailable:
            raise StorageNodeUnavailableError(f"{self.node_id} unavailable")
        body = self.storage[(self.node_id, version_id)]

        class Response:
            async def aiter_bytes(self):
                yield body

        yield Response()

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        if self.node_id in self.unavailable:
            raise StorageNodeUnavailableError(f"{self.node_id} unavailable")
        chunks = []
        async for chunk in data:
            chunks.append(bytes(chunk))
        self.storage[(self.node_id, version_id)] = b"".join(chunks)

    async def delete_object(self, object_id: str, version_id: str, **kwargs) -> None:
        self.storage.pop((self.node_id, version_id), None)

    async def aclose(self) -> None:
        return None


def _seed(db_session):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("failure.bin")
    version = metadata.create_version(
        obj.object_id,
        size_bytes=len(PAYLOAD),
        checksum=CHECKSUM,
    )

    nodes = {}
    for node_id in ("node-a", "node-b", "node-c", "node-d"):
        metadata.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=1024,
            status=NodeState.HEALTHY,
        )
        nodes[node_id] = InMemoryNode(node_id)

    for node_id in ("node-a", "node-b", "node-c"):
        replica = metadata.create_replica(version.version_id, node_id)
        metadata.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        metadata.mark_replica_healthy(
            replica.replica_id,
            checksum=CHECKSUM,
            size_bytes=len(PAYLOAD),
        )
        InMemoryNode.storage[(node_id, str(version.version_id))] = PAYLOAD

    metadata.commit_version(version.version_id)
    db_session.commit()
    return obj, version, nodes


@pytest.mark.asyncio
async def test_failure_detector_reaches_unavailable_and_marks_replicas(db_session):
    _, version, nodes = _seed(db_session)
    base = datetime.now(timezone.utc)

    manager = MetadataManager(db_session)
    manager.update_node_heartbeat(
        "node-b",
        capacity_bytes=1024,
        used_bytes=len(PAYLOAD),
        status=NodeState.HEALTHY,
        heartbeat_at=base - timedelta(seconds=31),
    )

    InMemoryNode.unavailable = {"node-b"}
    detector = FailureDetector(
        db_session,
        nodes,
        config=FailureDetectorConfig(
            suspect_after_seconds=15,
            unavailable_after_seconds=30,
        ),
        now=lambda: base,
    )

    result = await detector.probe_node("node-b")
    assert result.state is NodeState.UNAVAILABLE
    assert result.reachable is False
    assert result.replica_count_marked_unavailable == 1

    replica = db_session.scalar(
        select(Replica).where(
            Replica.version_id == version.version_id,
            Replica.node_id == "node-b",
        )
    )
    assert replica is not None
    assert replica.status is ReplicaState.UNAVAILABLE

    InMemoryNode.unavailable = set()


@pytest.mark.asyncio
async def test_failure_recovery_requires_two_healthy_probes(db_session):
    _, version, nodes = _seed(db_session)
    manager = MetadataManager(db_session)
    manager.set_node_status("node-b", NodeState.UNAVAILABLE)

    InMemoryNode.unavailable = set()
    detector = FailureDetector(db_session, nodes)

    recovering = await detector.probe_node("node-b")
    assert recovering.state is NodeState.RECOVERING

    healthy = await detector.probe_node("node-b")
    assert healthy.state is NodeState.HEALTHY

    assert db_session.scalar(
        select(StorageNode).where(StorageNode.node_id == "node-b")
    ).status is NodeState.HEALTHY

    replica = db_session.scalar(
        select(Replica).where(
            Replica.version_id == version.version_id,
            Replica.node_id == "node-b",
        )
    )
    assert replica is not None
    assert replica.status is ReplicaState.HEALTHY


@pytest.mark.asyncio
async def test_repair_worker_replaces_failed_replica_on_real_control_flow(db_session):
    obj, version, nodes = _seed(db_session)
    manager = MetadataManager(db_session)
    failed = db_session.scalar(
        select(Replica).where(
            Replica.version_id == version.version_id,
            Replica.node_id == "node-b",
        )
    )
    assert failed is not None
    manager.set_replica_state(failed.replica_id, ReplicaState.UNAVAILABLE)
    manager.set_node_status("node-b", NodeState.UNAVAILABLE)

    coordinator = DistributedWriteCoordinator(
        db_session,
        nodes,
        replication_factor=3,
        write_quorum=2,
        read_quorum=1,
    )
    worker = RepairWorker(db_session, coordinator)

    result = await worker.run_once()
    assert result.scheduled == 1
    assert result.succeeded == 1
    assert result.failed == 0

    job = db_session.scalar(select(RepairJob))
    assert job is not None
    assert job.status is JobStatus.SUCCEEDED
    assert job.target_node_id == "node-d"

    healthy = list(
        db_session.scalars(
            select(Replica).where(
                Replica.version_id == version.version_id,
                Replica.status == ReplicaState.HEALTHY,
            )
        )
    )
    assert {replica.node_id for replica in healthy} == {
        "node-a",
        "node-c",
        "node-d",
    }
    assert InMemoryNode.storage[("node-d", str(version.version_id))] == PAYLOAD
    assert obj.current_version_id == version.version_id

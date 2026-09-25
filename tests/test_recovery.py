from __future__ import annotations

from hashlib import sha256

import pytest

from common.constants import JobStatus, NodeState, ReplicaState, VersionState
from common.errors import InvalidState
from health.heartbeat import HeartbeatPayload, HeartbeatService
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, StorageNode
from replication.node_client import StorageNodeUnavailableError, StorageObjectNotFoundError
from recovery import PartitionRecoveryManager


DATA = b"partition-recovery-payload"
CHECKSUM = sha256(DATA).hexdigest()
BASE_TIME = "2026-09-25T20:00:00+00:00"


class FakeClient:
    storage: dict[tuple[str, str, str], bytes] = {}
    unavailable_nodes: set[str] = set()

    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        from replication.node_client import VerifiedObject

        if self.node_id in self.unavailable_nodes:
            raise StorageNodeUnavailableError("partition still active")
        body = self.storage.get((self.node_id, object_id, version_id))
        if body is None:
            raise StorageObjectNotFoundError("object missing")
        return VerifiedObject(
            object_id,
            version_id,
            len(body),
            sha256(body).hexdigest(),
            True,
            True,
        )

    async def aclose(self):
        return None


def _seed(db_session, *, include_node_b: bool = True):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("partition.txt")
    version = metadata.create_version(
        obj.object_id,
        size_bytes=len(DATA),
        checksum=CHECKSUM,
    )
    for node_id, capacity in (
        ("node-a", 1000),
        ("node-b", 900),
        ("node-c", 800),
    ):
        metadata.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=capacity,
            status=NodeState.HEALTHY,
        )

    source = metadata.create_replica(version.version_id, "node-a")
    metadata.set_replica_state(source.replica_id, ReplicaState.COPYING)
    metadata.mark_replica_healthy(
        source.replica_id,
        checksum=CHECKSUM,
        size_bytes=len(DATA),
    )

    if include_node_b:
        recovered = metadata.create_replica(version.version_id, "node-b")
        metadata.set_replica_state(recovered.replica_id, ReplicaState.COPYING)
        metadata.mark_replica_healthy(
            recovered.replica_id,
            checksum=CHECKSUM,
            size_bytes=len(DATA),
        )

    metadata.commit_version(version.version_id)
    FakeClient.storage = {}
    FakeClient.unavailable_nodes = set()
    FakeClient.storage[("node-a", str(obj.object_id), str(version.version_id))] = DATA
    if include_node_b:
        FakeClient.storage[("node-b", str(obj.object_id), str(version.version_id))] = DATA

    db_session.commit()
    return obj, version


@pytest.mark.asyncio
async def test_recovery_reconciles_known_replica_before_health(db_session):
    obj, version = _seed(db_session)
    metadata = MetadataManager(db_session)
    metadata.transition_node_state("node-b", NodeState.SUSPECT)
    metadata.transition_node_state("node-b", NodeState.UNAVAILABLE)

    heartbeat = HeartbeatService(
        db_session,
        recovery_manager_factory=lambda session, **kwargs: PartitionRecoveryManager(
            session,
            client_factory=FakeClient,
            **kwargs,
        ),
    )
    result = await heartbeat.ingest_and_recover(
        HeartbeatPayload(
            node_id="node-b",
            capacity_bytes=900,
            used_bytes=100,
            timestamp=BASE_TIME,
        )
    )

    assert result.accepted is True
    assert result.status is NodeState.HEALTHY
    node = db_session.get(StorageNode, "node-b")
    assert node.status is NodeState.HEALTHY
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()
    assert replica.status is ReplicaState.HEALTHY
    assert replica.last_verified_at is not None
    assert replica.checksum == CHECKSUM
    assert obj.current_version_id == version.version_id


@pytest.mark.asyncio
async def test_recovery_keeps_metadata_and_schedules_missing_replica_repair(db_session):
    obj, version = _seed(db_session)
    metadata = MetadataManager(db_session)
    metadata.transition_node_state("node-b", NodeState.SUSPECT)
    metadata.transition_node_state("node-b", NodeState.UNAVAILABLE)
    FakeClient.storage.pop(("node-b", str(obj.object_id), str(version.version_id)))

    service = HeartbeatService(
        db_session,
        recovery_manager_factory=lambda session, **kwargs: PartitionRecoveryManager(
            session,
            client_factory=FakeClient,
            **kwargs,
        ),
    )
    result = await service.ingest_and_recover(
        HeartbeatPayload(
            node_id="node-b",
            capacity_bytes=900,
            used_bytes=100,
            timestamp=BASE_TIME,
        )
    )

    assert result.status is NodeState.HEALTHY
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()
    assert replica.status is ReplicaState.UNAVAILABLE
    assert db_session.query(RepairJob).count() == 1
    job = db_session.query(RepairJob).one()
    assert job.status is JobStatus.PENDING
    assert job.target_node_id == "node-c"


@pytest.mark.asyncio
async def test_recovery_does_not_call_partition_a_corruption(db_session):
    _, version = _seed(db_session)
    metadata = MetadataManager(db_session)
    metadata.transition_node_state("node-b", NodeState.SUSPECT)
    metadata.transition_node_state("node-b", NodeState.UNAVAILABLE)
    FakeClient.unavailable_nodes = {"node-b"}

    service = HeartbeatService(
        db_session,
        recovery_manager_factory=lambda session, **kwargs: PartitionRecoveryManager(
            session,
            client_factory=FakeClient,
            **kwargs,
        ),
    )
    result = await service.ingest_and_recover(
        HeartbeatPayload(
            node_id="node-b",
            capacity_bytes=900,
            used_bytes=100,
            timestamp=BASE_TIME,
        )
    )

    assert result.status is NodeState.RECOVERING
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()
    assert replica.status is ReplicaState.HEALTHY
    assert db_session.query(RepairJob).count() == 0


def test_recovery_requires_recovering_node(db_session):
    _, version = _seed(db_session)
    with pytest.raises(InvalidState):
        import asyncio
        asyncio.run(
            PartitionRecoveryManager(
                db_session,
                client_factory=FakeClient,
            ).reconcile_node("node-b")
        )

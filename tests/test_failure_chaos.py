"""B13 failure/chaos regression tests for the complete control plane."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from hashlib import sha256

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from common.constants import JobStatus, NodeState, ReplicaState
from common.errors import VersionConflict
from health.failure_detector import FailureDetector
from health.heartbeat import HeartbeatPayload, HeartbeatService
from integrity import IntegrityManager
from metadata.database import Base
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, StorageNode
from recovery import PartitionRecoveryManager
from repair import RepairManager
from replication.node_client import (
    StorageNodeIntegrityError,
    StorageNodeUnavailableError,
)


DATA = b"vault-chaos-payload"
CHECKSUM = sha256(DATA).hexdigest()
BASE = datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc)


class ChaosClient:
    storage: dict[tuple[str, str, str], bytes] = {}
    unavailable_nodes: set[str] = set()

    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        from replication.node_client import VerifiedObject

        if self.node_id in self.unavailable_nodes:
            raise StorageNodeUnavailableError("partition")
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
        if self.node_id in self.unavailable_nodes:
            raise StorageNodeUnavailableError("partition")

        class Response:
            def __init__(self, body: bytes) -> None:
                self.body = body

            def aiter_bytes(self):
                return _aiter(self.body)

        yield Response(self.storage[(self.node_id, object_id, version_id)])

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        if self.node_id in self.unavailable_nodes:
            raise StorageNodeUnavailableError("partition")
        parts = []
        async for chunk in data:
            parts.append(bytes(chunk))
        body = b"".join(parts)
        self.storage[(self.node_id, object_id, version_id)] = body
        from replication.node_client import StoredObject
        return StoredObject(object_id, version_id, len(body))

    async def delete_object(self, object_id: str, version_id: str, **kwargs):
        if self.node_id in self.unavailable_nodes:
            raise StorageNodeUnavailableError("partition")
        self.storage.pop((self.node_id, object_id, version_id), None)

    async def aclose(self):
        return None


async def _aiter(body: bytes):
    yield body


def _seed_cluster(db_session):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("chaos.bin")
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
    ChaosClient.storage = {
        (node_id, str(obj.object_id), str(version.version_id)): DATA
        for node_id in ("node-a", "node-b", "node-c")
    }
    ChaosClient.storage.pop(("node-d", str(obj.object_id), str(version.version_id)), None)
    ChaosClient.unavailable_nodes = set()
    db_session.commit()
    return obj, version


@pytest.mark.asyncio
async def test_node_failure_repair_restores_rf_without_deleting_other_sources(db_session):
    obj, version = _seed_cluster(db_session)
    metadata = MetadataManager(db_session)
    failed = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()

    node = db_session.query(StorageNode).filter_by(node_id="node-b").one()
    node.last_heartbeat_at = BASE
    db_session.commit()

    detector = FailureDetector(
        db_session,
        suspect_after_seconds=15,
        unavailable_after_seconds=30,
    )
    first = detector.scan(now=BASE + timedelta(seconds=15))
    assert first and first[0].current is NodeState.SUSPECT
    second = detector.scan(now=BASE + timedelta(seconds=30))
    assert second and second[0].current is NodeState.UNAVAILABLE

    metadata.set_replica_state(failed.replica_id, ReplicaState.UNAVAILABLE)
    repair = RepairManager(db_session, client_factory=ChaosClient)
    job = repair.schedule_for_version(version.version_id, replication_factor=3)
    assert job is not None
    assert job.target_node_id == "node-d"

    result = await repair.run_job(job.repair_id)
    assert result.status is JobStatus.SUCCEEDED

    healthy = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        status=ReplicaState.HEALTHY,
    ).all()
    assert {r.node_id for r in healthy} == {"node-a", "node-c", "node-d"}
    assert ChaosClient.storage[("node-a", str(obj.object_id), str(version.version_id))] == DATA
    assert ChaosClient.storage[("node-c", str(obj.object_id), str(version.version_id))] == DATA


@pytest.mark.asyncio
async def test_corruption_scan_repairs_the_damaged_replica(db_session):
    obj, version = _seed_cluster(db_session)
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()
    ChaosClient.storage[("node-b", str(obj.object_id), str(version.version_id))] = b"CORRUPT"

    result = await IntegrityManager(
        db_session,
        client_factory=ChaosClient,
        replication_factor=3,
    ).verify_replica(replica.replica_id)

    assert result.corrupted is True
    assert result.repair_id is not None
    db_session.refresh(replica)
    assert replica.status is ReplicaState.CORRUPTED

    repaired = await RepairManager(
        db_session,
        client_factory=ChaosClient,
    ).run_job(result.repair_id)

    assert repaired.status is JobStatus.SUCCEEDED
    db_session.refresh(replica)
    assert replica.status is ReplicaState.HEALTHY
    assert ChaosClient.storage[("node-b", str(obj.object_id), str(version.version_id))] == DATA


@pytest.mark.asyncio
async def test_network_isolation_preserves_metadata_and_blocks_health_recovery(db_session):
    obj, version = _seed_cluster(db_session)
    metadata = MetadataManager(db_session)
    metadata.transition_node_state("node-b", NodeState.SUSPECT)
    metadata.transition_node_state("node-b", NodeState.UNAVAILABLE)
    ChaosClient.unavailable_nodes = {"node-b"}

    service = HeartbeatService(
        db_session,
        recovery_manager_factory=lambda session, **kwargs: PartitionRecoveryManager(
            session,
            client_factory=ChaosClient,
            **kwargs,
        ),
    )
    result = await service.ingest_and_recover(
        HeartbeatPayload(
            node_id="node-b",
            capacity_bytes=900,
            used_bytes=100,
            timestamp=BASE,
        )
    )

    assert result.status is NodeState.RECOVERING
    replica = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-b",
    ).one()
    assert replica.status is ReplicaState.HEALTHY
    assert db_session.query(StorageNode).filter_by(node_id="node-b").one().status is NodeState.RECOVERING
    assert obj.current_version_id == version.version_id


def test_competing_expected_versions_allow_only_one_commit(db_session):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("concurrent.bin")
    v1 = metadata.create_version(
        obj.object_id,
        size_bytes=1,
        checksum="a" * 64,
        expected_current_version=0,
    )
    metadata.commit_version(v1.version_id, expected_current_version=0)

    # Two writers both observed version 1. Each can stage a provisional version,
    # but only one may commit against expected_current_version=1.
    first = metadata.create_version(
        obj.object_id,
        size_bytes=2,
        checksum="b" * 64,
        expected_current_version=1,
    )
    second = metadata.create_version(
        obj.object_id,
        size_bytes=3,
        checksum="c" * 64,
        expected_current_version=1,
    )
    metadata.commit_version(first.version_id, expected_current_version=1)

    with pytest.raises(VersionConflict):
        metadata.commit_version(second.version_id, expected_current_version=1)


def test_repair_failure_state_survives_process_restart(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'vault.db'}"
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        metadata = MetadataManager(session)
        obj = metadata.create_object("restart.bin")
        version = metadata.create_version(
            obj.object_id,
            size_bytes=1,
            checksum="d" * 64,
        )
        metadata.register_node(
            node_id="source",
            address="http://source:9001",
            capacity_bytes=1000,
            status=NodeState.HEALTHY,
        )
        metadata.register_node(
            node_id="target",
            address="http://target:9001",
            capacity_bytes=1000,
            status=NodeState.HEALTHY,
        )
        job = RepairJob(
            version_id=version.version_id,
            source_node_id="source",
            target_node_id="target",
            status=JobStatus.PENDING,
            attempts=1,
            last_error="simulated restart-safe failure",
        )
        session.add(job)
        session.commit()
        repair_id = job.repair_id

    with Session() as session:
        restored = session.get(RepairJob, repair_id)
        assert restored is not None
        assert restored.status is JobStatus.PENDING
        assert restored.attempts == 1
        assert restored.last_error == "simulated restart-safe failure"

    engine.dispose()

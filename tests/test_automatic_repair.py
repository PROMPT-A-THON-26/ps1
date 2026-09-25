from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256

from common.constants import NodeState, ReplicaState
from health.failure_detector import FailureDetector
from health.heartbeat import HeartbeatPayload, HeartbeatService
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica


DATA = b"automatic-repair"
CHECKSUM = sha256(DATA).hexdigest()
BASE = datetime(2026, 9, 25, 20, 0, 0, tzinfo=timezone.utc)


def test_node_failure_marks_replicas_unavailable_and_creates_repair_jobs(db_session):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("auto-repair.txt")
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
    HeartbeatService(db_session).ingest(
        HeartbeatPayload(
            node_id="node-a",
            capacity_bytes=1000,
            used_bytes=100,
            timestamp=BASE,
        )
    )
    db_session.commit()

    detector = FailureDetector(
        db_session,
        suspect_after_seconds=15,
        unavailable_after_seconds=30,
        replication_factor=3,
    )
    first = detector.scan(now=BASE + timedelta(seconds=15))
    assert first and first[0].current is NodeState.SUSPECT
    transitions = detector.scan(now=BASE + timedelta(seconds=30))
    assert transitions
    db_session.expire_all()

    failed = db_session.query(Replica).filter_by(
        version_id=version.version_id,
        node_id="node-a",
    ).one()
    assert failed.status is ReplicaState.UNAVAILABLE

    job = db_session.query(RepairJob).one()
    assert job.source_node_id in {"node-b", "node-c"}
    assert job.target_node_id == "node-d"
    assert job.reason == "node-failure"

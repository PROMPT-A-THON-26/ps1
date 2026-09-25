from datetime import datetime, timedelta, timezone

from common.constants import NodeState
from health.failure_detector import FailureDetector
from metadata.manager import MetadataManager


def test_failure_detector_marks_suspect_then_unavailable(db_session):
    manager = MetadataManager(db_session)
    node = manager.register_node(
        node_id="node-health",
        address="http://node-health:9001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    heartbeat = datetime(2026, 1, 1, tzinfo=timezone.utc)
    manager.update_node_heartbeat(
        node.node_id,
        status=NodeState.HEALTHY,
        heartbeat_at=heartbeat,
    )

    detector = FailureDetector(
        db_session,
        suspect_after_seconds=15,
        unavailable_after_seconds=30,
    )

    changed = detector.evaluate(now=heartbeat + timedelta(seconds=15))
    assert changed == {"node-health": NodeState.SUSPECT}

    changed = detector.evaluate(now=heartbeat + timedelta(seconds=30))
    assert changed == {"node-health": NodeState.UNAVAILABLE}


def test_failure_detector_recovers_fresh_heartbeat_to_healthy(db_session):
    manager = MetadataManager(db_session)
    node = manager.register_node(
        node_id="node-recover",
        address="http://node-recover:9001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    heartbeat = datetime(2026, 1, 1, tzinfo=timezone.utc)
    manager.update_node_heartbeat(
        node.node_id,
        status=NodeState.UNAVAILABLE,
        heartbeat_at=heartbeat,
    )

    detector = FailureDetector(
        db_session,
        suspect_after_seconds=15,
        unavailable_after_seconds=30,
    )
    changed = detector.evaluate(now=heartbeat + timedelta(seconds=1))

    assert changed == {"node-recover": NodeState.HEALTHY}


def test_failure_detector_does_not_override_draining_or_removed_nodes(db_session):
    manager = MetadataManager(db_session)
    draining = manager.register_node(
        node_id="node-draining",
        address="http://node-draining:9001",
        capacity_bytes=1000,
        status=NodeState.DRAINING,
    )
    removed = manager.register_node(
        node_id="node-removed",
        address="http://node-removed:9001",
        capacity_bytes=1000,
        status=NodeState.REMOVED,
    )
    heartbeat = datetime(2026, 1, 1, tzinfo=timezone.utc)
    manager.update_node_heartbeat(draining.node_id, heartbeat_at=heartbeat)
    manager.update_node_heartbeat(removed.node_id, heartbeat_at=heartbeat)

    detector = FailureDetector(db_session, suspect_after_seconds=1, unavailable_after_seconds=2)
    changed = detector.evaluate(now=heartbeat + timedelta(seconds=10))

    assert changed == {}

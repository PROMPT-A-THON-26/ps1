from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.constants import NodeState
from common.errors import InvalidState
from health.failure_detector import FailureDetector
from health.heartbeat import HeartbeatPayload, HeartbeatService
from health.node_registry import NodeRegistry
from health.scheduler import HealthScheduler


BASE = datetime(2026, 9, 25, 20, 0, 0, tzinfo=timezone.utc)


def test_registry_registers_joining_node_and_filters_healthy_capacity(db_session):
    registry = NodeRegistry(db_session)
    node1 = registry.register(node_id="node-01", address="http://node-01:8001", capacity_bytes=1000)
    node2 = registry.register(node_id="node-02", address="http://node-02:8001", capacity_bytes=500)
    assert node1.status is NodeState.JOINING
    assert node2.status is NodeState.JOINING

    registry.mark_joined(node1.node_id)
    registry.mark_joined(node2.node_id)
    db_session.refresh(node1)
    db_session.refresh(node2)
    node2.used_bytes = 450
    db_session.commit()

    selected = registry.healthy_nodes(min_free_bytes=600)
    assert [node.node_id for node in selected] == ["node-01"]
    assert registry.healthy_nodes(exclude_node_ids=["node-01"])[0].node_id == "node-02"


def test_node_state_machine_allows_only_canonical_transitions(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-state", address="http://node-state:8001", capacity_bytes=1000)

    with pytest.raises(InvalidState):
        registry.mark_unavailable("node-state")

    registry.mark_joined("node-state")
    registry.mark_suspect("node-state")
    registry.mark_unavailable("node-state")
    registry.mark_recovering("node-state")
    registry.mark_healthy("node-state")
    registry.drain("node-state")
    registry.remove("node-state")

    with pytest.raises(InvalidState):
        registry.mark_joined("node-state")


def test_heartbeat_rejects_stale_packet_without_overwriting_state_or_capacity(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-heartbeat", address="http://node-heartbeat:8001", capacity_bytes=1000)
    service = HeartbeatService(db_session)

    first = service.ingest(HeartbeatPayload(
        node_id="node-heartbeat", capacity_bytes=1200, used_bytes=200, timestamp=BASE
    ))
    assert first.accepted is True
    assert first.status is NodeState.HEALTHY

    registry.mark_suspect("node-heartbeat")
    stale = service.ingest(HeartbeatPayload(
        node_id="node-heartbeat", capacity_bytes=2000, used_bytes=500,
        timestamp=BASE - timedelta(seconds=1)
    ))
    assert stale.accepted is False
    assert stale.status is NodeState.SUSPECT
    assert stale.capacity_bytes == 1200
    assert stale.used_bytes == 200

    fresh = service.ingest(HeartbeatPayload(
        node_id="node-heartbeat", capacity_bytes=2000, used_bytes=500,
        timestamp=BASE + timedelta(seconds=1)
    ))
    assert fresh.accepted is True
    assert fresh.status is NodeState.HEALTHY
    assert fresh.free_bytes == 1500


@pytest.mark.asyncio
async def test_unavailable_node_requires_recovery_heartbeat_then_health(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-recovery", address="http://node-recovery:8001", capacity_bytes=1000)
    service = HeartbeatService(db_session)
    service.ingest(HeartbeatPayload(
        node_id="node-recovery", capacity_bytes=1000, used_bytes=100, timestamp=BASE
    ))
    registry.mark_suspect("node-recovery")
    registry.mark_unavailable("node-recovery")

    recovering = service.ingest(HeartbeatPayload(
        node_id="node-recovery", capacity_bytes=1000, used_bytes=100,
        timestamp=BASE + timedelta(seconds=31)
    ))
    assert recovering.status is NodeState.RECOVERING

    healthy = await service.ingest_and_recover(HeartbeatPayload(
        node_id="node-recovery", capacity_bytes=1000, used_bytes=100,
        timestamp=BASE + timedelta(seconds=32)
    ))
    assert healthy.status is NodeState.HEALTHY


def test_heartbeat_payload_rejects_used_over_capacity():
    with pytest.raises(ValueError):
        HeartbeatPayload(
            node_id="node",
            capacity_bytes=10,
            used_bytes=11,
            timestamp=BASE,
        )


def test_failure_detector_uses_thresholds(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-failure", address="http://node-failure:8001", capacity_bytes=1000)
    HeartbeatService(db_session).ingest(HeartbeatPayload(
        node_id="node-failure", capacity_bytes=1000, used_bytes=100, timestamp=BASE
    ))

    detector = FailureDetector(db_session, suspect_after_seconds=15, unavailable_after_seconds=30)
    first = detector.scan(now=BASE + timedelta(seconds=15))
    assert [(t.previous, t.current) for t in first] == [(NodeState.HEALTHY, NodeState.SUSPECT)]

    second = detector.scan(now=BASE + timedelta(seconds=30))
    assert [(t.previous, t.current) for t in second] == [(NodeState.SUSPECT, NodeState.UNAVAILABLE)]


def test_failure_detector_does_not_fail_joining_node_without_a_heartbeat(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-no-heartbeat", address="http://node-no-heartbeat:8001", capacity_bytes=1000)
    detector = FailureDetector(db_session, suspect_after_seconds=1, unavailable_after_seconds=2)
    assert detector.scan(now=BASE + timedelta(seconds=100)) == []
    assert registry.get("node-no-heartbeat").status is NodeState.JOINING


def test_failure_detector_ignores_future_heartbeat(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-future", address="http://node-future:8001", capacity_bytes=1000)
    HeartbeatService(db_session).ingest(HeartbeatPayload(
        node_id="node-future", capacity_bytes=1000, used_bytes=1,
        timestamp=BASE + timedelta(seconds=60)
    ))
    detector = FailureDetector(db_session, suspect_after_seconds=15, unavailable_after_seconds=30)
    assert detector.scan(now=BASE) == []
    assert registry.get("node-future").status is NodeState.HEALTHY


@pytest.mark.asyncio
async def test_scheduler_runs_one_scan(db_session):
    registry = NodeRegistry(db_session)
    registry.register(node_id="node-scheduler", address="http://node-scheduler:8001", capacity_bytes=1000)
    HeartbeatService(db_session).ingest(HeartbeatPayload(
        node_id="node-scheduler", capacity_bytes=1000, used_bytes=100, timestamp=BASE
    ))
    detector = FailureDetector(db_session, suspect_after_seconds=15, unavailable_after_seconds=30)
    scheduler = HealthScheduler(detector, interval_seconds=5)
    transitions = scheduler.run_once(now=BASE + timedelta(seconds=15))
    assert transitions and transitions[0].current is NodeState.SUSPECT

from __future__ import annotations

from uuid import uuid4

import pytest

from common.constants import ErrorCode, NodeState
from common.errors import VaultError
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode
from placement import PlacementManager, PlacementPolicy


SHA_A = "a" * 64


def _version_with_nodes(db_session, *, size_bytes: int = 100) -> tuple[MetadataManager, object, object]:
    manager = MetadataManager(db_session)
    obj = manager.create_object("placement-object.bin")
    version = manager.create_version(obj.object_id, size_bytes=size_bytes, checksum=SHA_A)
    return manager, obj, version


def _register(
    manager: MetadataManager,
    node_id: str,
    *,
    capacity: int,
    used: int = 0,
    status: NodeState = NodeState.HEALTHY,
) -> StorageNode:
    node = manager.register_node(
        node_id=node_id,
        address=f"http://{node_id}:9001",
        capacity_bytes=capacity,
        status=status,
    )
    node.used_bytes = used
    manager.session.commit()
    return node


def test_selects_default_replication_factor_from_policy(db_session):
    manager, _, version = _version_with_nodes(db_session)
    for node_id in ("node-a", "node-b", "node-c"):
        _register(manager, node_id, capacity=1_000)

    selected = PlacementManager(db_session).plan_for_version(
        version.version_id, size_bytes=version.size_bytes
    )

    assert len(selected) == 3
    assert set(selected) == {"node-a", "node-b", "node-c"}


def test_selects_capacity_descending_with_node_id_tie_break(db_session):
    manager, _, version = _version_with_nodes(db_session)
    _register(manager, "node-b", capacity=1_000, used=200)  # free 800
    _register(manager, "node-a", capacity=1_000, used=200)  # free 800
    _register(manager, "node-c", capacity=1_000, used=50)   # free 950

    selected = PlacementManager(
        db_session, policy=PlacementPolicy(replication_factor=3)
    ).plan_for_version(version.version_id, size_bytes=version.size_bytes)

    assert selected == ["node-c", "node-a", "node-b"]


def test_excludes_unhealthy_nodes(db_session):
    manager, _, version = _version_with_nodes(db_session)
    _register(manager, "healthy-1", capacity=1_000)
    _register(manager, "suspect-1", capacity=9_000, status=NodeState.SUSPECT)
    _register(manager, "healthy-2", capacity=1_000)
    _register(manager, "healthy-3", capacity=1_000)

    selected = PlacementManager(db_session).plan_for_version(
        version.version_id, size_bytes=version.size_bytes
    )

    assert "suspect-1" not in selected
    assert selected == ["healthy-1", "healthy-2", "healthy-3"]


def test_excludes_nodes_without_enough_free_capacity(db_session):
    manager, _, version = _version_with_nodes(db_session, size_bytes=500)
    _register(manager, "too-small", capacity=499)
    _register(manager, "enough-1", capacity=500)
    _register(manager, "enough-2", capacity=700)
    _register(manager, "enough-3", capacity=800)

    selected = PlacementManager(db_session).plan_for_version(
        version.version_id, size_bytes=version.size_bytes
    )

    assert selected == ["enough-3", "enough-2", "enough-1"]


def test_excludes_nodes_that_already_have_the_version(db_session):
    manager, _, version = _version_with_nodes(db_session)
    _register(manager, "existing", capacity=10_000)
    _register(manager, "node-2", capacity=2_000)
    _register(manager, "node-3", capacity=2_000)
    _register(manager, "node-4", capacity=2_000)
    manager.create_replica(version.version_id, "existing")

    selected = PlacementManager(db_session).plan_for_version(
        version.version_id, size_bytes=version.size_bytes
    )

    assert selected == ["node-2", "node-3", "node-4"]


def test_raises_insufficient_replicas_without_returning_partial_plan(db_session):
    manager, _, version = _version_with_nodes(db_session)
    _register(manager, "node-1", capacity=1_000)
    _register(manager, "node-2", capacity=1_000)

    with pytest.raises(VaultError) as exc_info:
        PlacementManager(db_session).select_nodes(
            version.version_id, size_bytes=version.size_bytes
        )

    assert exc_info.value.code is ErrorCode.INSUFFICIENT_REPLICAS
    assert exc_info.value.status_code == 503
    assert "2 eligible" in str(exc_info.value)


def test_replication_factor_can_be_overridden_without_mutating_policy(db_session):
    manager, _, version = _version_with_nodes(db_session)
    for node_id in ("node-a", "node-b", "node-c", "node-d"):
        _register(manager, node_id, capacity=1_000)

    placement = PlacementManager(
        db_session, policy=PlacementPolicy(replication_factor=3)
    )
    selected = placement.plan_for_version(
        version.version_id, size_bytes=version.size_bytes, replication_factor=2
    )

    assert selected == ["node-a", "node-b"]
    assert placement.policy.replication_factor == 3


def test_policy_rejects_invalid_replication_factor(db_session):
    with pytest.raises(ValueError):
        PlacementPolicy(replication_factor=0)
    with pytest.raises(ValueError):
        PlacementPolicy(replication_factor=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        PlacementPolicy(replication_factor=-1)


def test_placement_rejects_invalid_version_and_size(db_session):
    placement = PlacementManager(db_session)

    with pytest.raises(ValueError):
        placement.select_nodes(uuid4(), size_bytes=-1)
    with pytest.raises(ValueError):
        placement.select_nodes("not-a-uuid", size_bytes=1)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        placement.select_nodes(uuid4(), size_bytes=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        placement.select_nodes(uuid4(), size_bytes=1, replication_factor=0)


def test_placement_only_plans_and_does_not_create_replica_records(db_session):
    manager, _, version = _version_with_nodes(db_session)
    for node_id in ("node-a", "node-b", "node-c"):
        _register(manager, node_id, capacity=1_000)

    selected = PlacementManager(db_session).plan_for_version(
        version.version_id, size_bytes=version.size_bytes
    )

    assert len(selected) == 3
    assert manager.session.query(Replica).count() == 0

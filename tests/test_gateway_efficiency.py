from __future__ import annotations

from common.constants import NodeState
from gateway.service import GatewayService
from metadata.manager import MetadataManager
from common.constants import ReplicaState


SHA_A = "a" * 64
SHA_B = "b" * 64


def test_version_history_reports_replica_counts_in_batch(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("history.bin")
    nodes = [
        manager.register_node(
            node_id=f"node-v{i}",
            address=f"http://node-v{i}:9001",
            capacity_bytes=10_000,
            status=NodeState.HEALTHY,
        )
        for i in range(3)
    ]

    first = manager.create_version(obj.object_id, size_bytes=10, checksum=SHA_A)
    for node in nodes:
        replica = manager.create_replica(first.version_id, node.node_id)
        manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        manager.mark_replica_healthy(
            replica.replica_id,
            checksum=SHA_A,
            size_bytes=10,
        )
    manager.commit_version(first.version_id)

    second = manager.create_version(obj.object_id, size_bytes=20, checksum=SHA_B)
    for node in nodes[:2]:
        replica = manager.create_replica(second.version_id, node.node_id)
        manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        manager.mark_replica_healthy(
            replica.replica_id,
            checksum=SHA_B,
            size_bytes=20,
        )

    result = GatewayService(db_session).versions("history.bin")

    assert [item["version_number"] for item in result] == [1, 2]
    assert [item["healthy_replicas"] for item in result] == [3, 2]

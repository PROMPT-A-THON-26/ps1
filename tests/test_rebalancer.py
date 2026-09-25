from __future__ import annotations

from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import JobStatus, NodeState, ReplicaState
from metadata.manager import MetadataManager
from metadata.models import RebalanceJob, Replica
from rebalance.rebalancer import RebalancePolicy, Rebalancer
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import (
    NodeStats,
    RetryPolicy,
    StorageNodeClient,
    StorageNodeClientConfig,
)
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


class ToggleTransport(httpx.AsyncBaseTransport):
    def __init__(self, app) -> None:
        self.inner = httpx.ASGITransport(app=app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()


class StatsOverrideClient:
    def __init__(self, inner, *, used_bytes: int, capacity_bytes: int = 100) -> None:
        self.inner = inner
        self.used_bytes = used_bytes
        self.capacity_bytes = capacity_bytes

    async def health(self):
        return await self.inner.health()

    async def stats(self):
        return NodeStats(
            node_id=self.inner.config.address.rsplit("//", 1)[-1],
            capacity_bytes=self.capacity_bytes,
            used_bytes=self.used_bytes,
            free_bytes=self.capacity_bytes - self.used_bytes,
        )

    async def verify_object(self, *args, **kwargs):
        return await self.inner.verify_object(*args, **kwargs)

    def stream_object(self, *args, **kwargs):
        return self.inner.stream_object(*args, **kwargs)

    async def put_object(self, *args, **kwargs):
        return await self.inner.put_object(*args, **kwargs)

    async def delete_object(self, *args, **kwargs):
        return await self.inner.delete_object(*args, **kwargs)


async def build_nodes(tmp_path: Path, stack: AsyncExitStack):
    nodes = {}
    for node_id in ("node-1", "node-2", "node-3", "node-4"):
        engine = StorageEngine(
            tmp_path / node_id,
            capacity_bytes=100,
            chunk_size_bytes=4,
        )
        app = create_storage_node_app(engine, node_id, NodeLifecycle())
        transport = ToggleTransport(app)
        client_transport = await stack.enter_async_context(
            httpx.AsyncClient(transport=transport, base_url=f"http://{node_id}")
        )
        client = StorageNodeClient(
            StorageNodeClientConfig(
                f"http://{node_id}",
                retry_policy=RetryPolicy(
                    max_attempts=1,
                    base_delay_seconds=0,
                    max_delay_seconds=0,
                    jitter_ratio=0,
                ),
            ),
            client=client_transport,
        )
        await stack.enter_async_context(client)
        nodes[node_id] = client
    return nodes


@pytest.mark.asyncio
async def test_rebalancer_moves_replica_only_after_verified_destination(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        raw_nodes = await build_nodes(tmp_path, stack)
        nodes = {
            "node-1": StatsOverrideClient(raw_nodes["node-1"], used_bytes=90),
            "node-2": StatsOverrideClient(raw_nodes["node-2"], used_bytes=40),
            "node-3": StatsOverrideClient(raw_nodes["node-3"], used_bytes=50),
            "node-4": StatsOverrideClient(raw_nodes["node-4"], used_bytes=10),
        }

        manager = MetadataManager(db_session)
        for node_id in nodes:
            manager.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=100,
                status=NodeState.HEALTHY,
            )

        coordinator = DistributedWriteCoordinator(
            db_session, raw_nodes, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("rebalance.bin", b"rebalancing-data")

        rebalancer = Rebalancer(
            db_session,
            nodes,
            replication_factor=3,
            policy=RebalancePolicy(
                high_watermark=0.80, low_watermark=0.60, max_moves_per_scan=1
            ),
        )

        outcome = await rebalancer.run_once()
        assert outcome.scanned_nodes == 4
        assert outcome.planned_jobs == 1
        assert outcome.completed_jobs == 1
        assert outcome.failed_jobs == 0

        job = db_session.scalar(select(RebalanceJob))
        assert job is not None
        assert job.status is JobStatus.SUCCEEDED
        assert job.source_node_id == "node-1"
        assert job.target_node_id == "node-4"

        replicas = list(
            db_session.scalars(
                select(Replica).where(Replica.version_id == result.version_id)
            )
        )
        assert {r.node_id for r in replicas} == {"node-2", "node-3", "node-4"}
        assert {r.status for r in replicas} == {ReplicaState.HEALTHY}

        assert await raw_nodes["node-4"].verify_object(
            str(result.object_id), str(result.version_id)
        )

        source_path = (
            tmp_path
            / "node-1"
            / "objects"
            / str(result.object_id)
            / str(result.version_id)
        )
        assert not source_path.exists()
        assert await coordinator.read_object("rebalance.bin") == b"rebalancing-data"


@pytest.mark.asyncio
async def test_rebalancer_does_not_move_when_durability_is_already_degraded(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        raw_nodes = await build_nodes(tmp_path, stack)
        nodes = {
            node_id: StatsOverrideClient(
                client, used_bytes=90 if node_id == "node-1" else 10
            )
            for node_id, client in raw_nodes.items()
        }
        manager = MetadataManager(db_session)
        for node_id in nodes:
            manager.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=100,
                status=NodeState.HEALTHY,
            )

        coordinator = DistributedWriteCoordinator(
            db_session, raw_nodes, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("degraded.bin", b"durability-first")
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        manager.set_replica_state(replica.replica_id, ReplicaState.UNAVAILABLE)

        rebalancer = Rebalancer(db_session, nodes, replication_factor=3)
        outcome = await rebalancer.run_once()

        assert outcome.planned_jobs == 0
        assert list(db_session.scalars(select(RebalanceJob))) == []
        assert replica.status is ReplicaState.UNAVAILABLE


def test_rebalance_policy_rejects_invalid_watermarks():
    with pytest.raises(ValueError):
        RebalancePolicy(high_watermark=0.5, low_watermark=0.5)
    with pytest.raises(ValueError):
        RebalancePolicy(max_moves_per_scan=0)

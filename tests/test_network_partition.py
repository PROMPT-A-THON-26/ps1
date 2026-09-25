from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import NodeState, ReplicaState
from health.failure_detector import FailureDetector, FailureDetectorConfig
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


class SelectivePartitionTransport(httpx.AsyncBaseTransport):
    """Inject directional/network-path failures without changing the node itself."""

    def __init__(self, app) -> None:
        self._inner = httpx.ASGITransport(app=app)
        self.block_all = False
        self.block_health = False
        self.block_stats = False
        self.block_put = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path

        blocked = (
            self.block_all
            or (self.block_health and path == "/internal/v1/health")
            or (self.block_stats and path == "/internal/v1/stats")
            or (self.block_put and request.method == "PUT")
        )
        if blocked:
            raise httpx.ConnectError("simulated network partition", request=request)

        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


async def build_node(tmp_path: Path, stack: AsyncExitStack, node_id: str = "node-1"):
    engine = StorageEngine(
        tmp_path / node_id,
        capacity_bytes=10_000_000,
        chunk_size_bytes=4,
    )
    app = create_storage_node_app(engine, node_id, NodeLifecycle())

    partition_transport = SelectivePartitionTransport(app)
    partition_client_transport = await stack.enter_async_context(
        httpx.AsyncClient(
            transport=partition_transport,
            base_url=f"http://{node_id}-partitioned",
        )
    )
    partitioned_client = StorageNodeClient(
        StorageNodeClientConfig(
            f"http://{node_id}-partitioned",
            retry_policy=RetryPolicy(
                max_attempts=1,
                base_delay_seconds=0,
                max_delay_seconds=0,
                jitter_ratio=0,
            ),
        ),
        client=partition_client_transport,
    )
    await stack.enter_async_context(partitioned_client)

    direct_transport = await stack.enter_async_context(
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=f"http://{node_id}-direct",
        )
    )
    direct_client = StorageNodeClient(
        StorageNodeClientConfig(
            f"http://{node_id}-direct",
            retry_policy=RetryPolicy(
                max_attempts=1,
                base_delay_seconds=0,
                max_delay_seconds=0,
                jitter_ratio=0,
            ),
        ),
        client=direct_transport,
    )
    await stack.enter_async_context(direct_client)

    return partitioned_client, direct_client, partition_transport


def register_node(session, node_id: str = "node-1") -> None:
    MetadataManager(session).register_node(
        node_id=node_id,
        address=f"http://{node_id}:9001",
        capacity_bytes=10_000_000,
    )


@pytest.mark.asyncio
async def test_stats_only_partition_does_not_age_liveness(db_session, tmp_path):
    async with AsyncExitStack() as stack:
        client, _direct, transport = await build_node(tmp_path, stack)
        register_node(db_session)

        clock = [datetime.now(timezone.utc) + timedelta(seconds=60)]
        detector = FailureDetector(
            db_session,
            {"node-1": client},
            config=FailureDetectorConfig(
                interval_seconds=1,
                suspect_after_seconds=10,
                unavailable_after_seconds=20,
            ),
            now=lambda: clock[0],
        )

        first = await detector.probe_node("node-1")
        assert first.state is NodeState.HEALTHY
        assert first.reachable is True
        assert first.stats_reachable is True

        transport.block_stats = True
        clock[0] += timedelta(seconds=15)

        second = await detector.probe_node("node-1")

        assert second.state is NodeState.HEALTHY
        assert second.reachable is True
        assert second.stats_reachable is False

        node = db_session.scalar(
            select(StorageNode).where(StorageNode.node_id == "node-1")
        )
        assert node is not None
        assert node.status is NodeState.HEALTHY
        assert node.last_heartbeat_at is not None
        assert node.last_heartbeat_at.replace(tzinfo=timezone.utc) == clock[0]


@pytest.mark.asyncio
async def test_control_plane_partition_detects_suspect_then_unavailable_while_node_stays_alive(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        partitioned, direct, transport = await build_node(tmp_path, stack)
        register_node(db_session)

        base = datetime.now(timezone.utc) + timedelta(seconds=60)
        clock = [base]
        detector = FailureDetector(
            db_session,
            {"node-1": partitioned},
            config=FailureDetectorConfig(
                interval_seconds=1,
                suspect_after_seconds=10,
                unavailable_after_seconds=20,
            ),
            now=lambda: clock[0],
        )

        healthy = await detector.probe_node("node-1")
        assert healthy.state is NodeState.HEALTHY

        transport.block_all = True
        clock[0] = base + timedelta(seconds=15)

        suspect = await detector.probe_node("node-1")
        assert suspect.state is NodeState.SUSPECT
        assert suspect.reachable is False

        # The node itself is still alive. Only the control-plane network path
        # is partitioned.
        direct_health = await direct.health()
        assert direct_health.status.lower() == NodeState.HEALTHY.value.lower()
        assert direct_health.node_id == "node-1"

        clock[0] = base + timedelta(seconds=25)
        unavailable = await detector.probe_node("node-1")
        assert unavailable.state is NodeState.UNAVAILABLE

        transport.block_all = False
        clock[0] = base + timedelta(seconds=26)

        recovering = await detector.probe_node("node-1")
        assert recovering.state is NodeState.RECOVERING

        clock[0] = base + timedelta(seconds=27)
        recovered = await detector.probe_node("node-1")
        assert recovered.state is NodeState.HEALTHY


@pytest.mark.asyncio
async def test_write_path_partition_does_not_evict_healthy_node(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients = {}
        transports = {}

        for node_id in ("node-1", "node-2", "node-3", "node-4"):
            engine = StorageEngine(
                tmp_path / node_id,
                capacity_bytes=10_000_000,
                chunk_size_bytes=4,
            )
            app = create_storage_node_app(engine, node_id, NodeLifecycle())
            transport = SelectivePartitionTransport(app)
            client_transport = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=transport,
                    base_url=f"http://{node_id}",
                )
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
            clients[node_id] = client
            transports[node_id] = transport

        manager = MetadataManager(db_session)
        for node_id in clients:
            manager.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=10_000_000,
            )

        transports["node-1"].block_put = True

        coordinator = DistributedWriteCoordinator(
            db_session,
            clients,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )

        result = await coordinator.write_object("partitioned-write.bin", b"partition-test")

        assert result.healthy_nodes == ("node-2", "node-3")
        assert result.failed_nodes == ("node-1",)

        node = db_session.scalar(
            select(StorageNode).where(StorageNode.node_id == "node-1")
        )
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )

        assert node is not None
        assert node.status is NodeState.HEALTHY
        assert replica is not None
        assert replica.status is ReplicaState.FAILED

        # The node is still reachable through its health path and has not been
        # falsely classified as unavailable just because PUT traffic is broken.
        health = await clients["node-1"].health()
        assert health.status.lower() == NodeState.HEALTHY.value.lower()

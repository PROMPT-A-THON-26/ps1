from __future__ import annotations

from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import InsufficientReplicas
from metadata.manager import MetadataManager
from metadata.models import Replica, Version
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


class ToggleTransport(httpx.AsyncBaseTransport):
    """ASGI transport that can simulate a hard node/network failure."""

    def __init__(self, app) -> None:
        self._inner = httpx.ASGITransport(app=app)
        self.down = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            raise httpx.ConnectError("simulated node failure", request=request)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


async def build_nodes(tmp_path: Path, stack: AsyncExitStack):
    nodes = {}
    transports = {}
    for node_id in ("node-1", "node-2", "node-3", "node-4"):
        engine = StorageEngine(
            tmp_path / node_id,
            capacity_bytes=10_000_000,
            chunk_size_bytes=4,
        )
        lifecycle = NodeLifecycle()
        app = create_storage_node_app(engine, node_id, lifecycle)
        transport = ToggleTransport(app)
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
        nodes[node_id] = client
        transports[node_id] = transport

    return nodes, transports


@pytest.mark.asyncio
async def test_end_to_end_quorum_write_failover_and_repair(db_session, tmp_path):
    async with AsyncExitStack() as stack:
        nodes, transports = await build_nodes(tmp_path, stack)
        manager = MetadataManager(db_session)

        for node_id in nodes:
            manager.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=10_000_000,
            )

        coordinator = DistributedWriteCoordinator(
            db_session,
            nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )

        payload = b"fault-tolerant-vault-object"
        result = await coordinator.write_object("object.bin", payload)

        assert len(result.healthy_nodes) == 3
        assert result.failed_nodes == ()
        assert coordinator.read_quorum == 1

        version = db_session.scalar(
            select(Version).where(Version.version_id == result.version_id)
        )
        assert version is not None
        assert version.state is VersionState.COMMITTED

        replicas = list(
            db_session.scalars(
                select(Replica).where(Replica.version_id == result.version_id)
            )
        )
        assert len(replicas) == 3
        assert {replica.status for replica in replicas} == {ReplicaState.HEALTHY}

        assert await coordinator.read_object("object.bin") == payload

        transports["node-1"].down = True

        # The read path detects the unavailable replica and fails over to a
        # surviving healthy replica without waiting for repair.
        assert await coordinator.read_object("object.bin") == payload

        failed_replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert failed_replica is not None
        assert failed_replica.status is ReplicaState.UNAVAILABLE

        repaired = await coordinator.repair_version(
            result.version_id,
            failed_node_id="node-1",
        )
        assert repaired.target_node_id == "node-4"
        assert repaired.healthy_replica_count == 3

        final_replicas = list(
            db_session.scalars(
                select(Replica).where(Replica.version_id == result.version_id)
            )
        )
        healthy = {
            replica.node_id
            for replica in final_replicas
            if replica.status is ReplicaState.HEALTHY
        }
        assert healthy == {"node-2", "node-3", "node-4"}

        assert await coordinator.read_object("object.bin") == payload


@pytest.mark.asyncio
async def test_end_to_end_corrupt_replica_is_skipped(db_session, tmp_path):
    async with AsyncExitStack() as stack:
        nodes, _transports = await build_nodes(tmp_path, stack)
        manager = MetadataManager(db_session)

        for node_id in nodes:
            manager.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=10_000_000,
            )

        coordinator = DistributedWriteCoordinator(
            db_session,
            nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )
        payload = b"corruption-survival"
        result = await coordinator.write_object("corrupt.bin", payload)

        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None

        # Deliberately corrupt one actual stored chunk after a successful
        # quorum write. The coordinator must detect it during VERIFY and
        # transparently use a healthy replica.
        version_dir = (
            tmp_path
            / "node-1"
            / "objects"
            / str(result.object_id)
            / str(result.version_id)
        )
        chunk = version_dir / "chunk-000000"
        chunk.write_bytes(b"XXXX")

        assert await coordinator.read_object("corrupt.bin") == payload
        db_session.expire_all()

        replica = db_session.scalar(
            select(Replica).where(Replica.replica_id == replica.replica_id)
        )
        assert replica.status is ReplicaState.CORRUPTED


@pytest.mark.asyncio
async def test_quorum_failure_aborts_version(db_session, tmp_path):
    async with AsyncExitStack() as stack:
        nodes, transports = await build_nodes(tmp_path, stack)
        manager = MetadataManager(db_session)

        for node_id in nodes:
            manager.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=10_000_000,
            )

        transports["node-1"].down = True
        transports["node-2"].down = True
        transports["node-3"].down = True

        coordinator = DistributedWriteCoordinator(
            db_session,
            nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )

        with pytest.raises(InsufficientReplicas):
            await coordinator.write_object("quorum-failure.bin", b"must-not-commit")

        failed = db_session.scalar(
            select(Version).join_from(Version, Version.object).where(
                Version.state is VersionState.FAILED
            )
        )
        assert failed is not None

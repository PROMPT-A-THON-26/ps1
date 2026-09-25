from __future__ import annotations

from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import NodeState, ReplicaState
from health.failure_detector import FailureDetector
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
from replication.reconciler import PartitionReconciler
from repair.worker import RepairWorker
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


class Transport(httpx.AsyncBaseTransport):
    def __init__(self, app):
        self._inner = httpx.ASGITransport(app=app)
        self.down = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.down:
            raise httpx.ConnectError("partition", request=request)
        return await self._inner.handle_async_request(request)

    async def aclose(self):
        await self._inner.aclose()


async def build_nodes(tmp_path: Path, stack: AsyncExitStack):
    clients, transports = {}, {}
    for node_id in ("node-1", "node-2", "node-3", "node-4"):
        engine = StorageEngine(
            tmp_path / node_id, capacity_bytes=10_000_000, chunk_size_bytes=4
        )
        app = create_storage_node_app(engine, node_id, NodeLifecycle())
        transport = Transport(app)
        http = await stack.enter_async_context(
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
            client=http,
        )
        await stack.enter_async_context(client)
        clients[node_id] = client
        transports[node_id] = transport
    return clients, transports


def register(session, clients):
    manager = MetadataManager(session)
    for node_id in clients:
        manager.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=10_000_000,
        )


@pytest.mark.asyncio
async def test_reconciliation_marks_divergent_replica_corrupted_without_overwriting(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients, _transports = await build_nodes(tmp_path, stack)
        register(db_session, clients)

        coordinator = DistributedWriteCoordinator(
            db_session, clients, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("reconcile.bin", b"canonical-data")

        await clients["node-1"].delete_object(
            str(result.object_id), str(result.version_id)
        )
        await clients["node-1"].put_object(
            str(result.object_id), str(result.version_id), b"divergent-data"
        )

        reconciler = PartitionReconciler(db_session, clients)
        reconciliation = await reconciler.reconcile_version(result.version_id)

        assert reconciliation.checked == 3
        assert reconciliation.healthy == 2
        assert reconciliation.corrupted == 1

        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        assert replica.status is ReplicaState.CORRUPTED

        assert await coordinator.read_object("reconcile.bin") == b"canonical-data"
        verified = await clients["node-2"].verify_object(
            str(result.object_id), str(result.version_id)
        )
        assert verified.valid is True


@pytest.mark.asyncio
async def test_reconciliation_restores_verified_replica_after_partition_heals(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients, transports = await build_nodes(tmp_path, stack)
        register(db_session, clients)

        coordinator = DistributedWriteCoordinator(
            db_session, clients, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("partition.bin", b"partition-data")

        detector = FailureDetector(db_session, clients)
        transports["node-1"].down = True
        await detector.probe_node("node-1")
        transports["node-1"].down = False

        await detector.probe_node("node-1")
        await detector.probe_node("node-1")

        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        replica.status = ReplicaState.UNAVAILABLE

        reconciler = PartitionReconciler(db_session, clients)
        repaired_state = await reconciler.reconcile_version(result.version_id)

        assert repaired_state.healthy == 3
        assert repaired_state.unavailable == 0

        db_session.expire_all()
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        assert replica.status is ReplicaState.HEALTHY


@pytest.mark.asyncio
async def test_missing_replica_after_partition_is_repaired_to_new_node(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients, _transports = await build_nodes(tmp_path, stack)
        register(db_session, clients)

        coordinator = DistributedWriteCoordinator(
            db_session, clients, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("missing.bin", b"missing-data")

        await clients["node-1"].delete_object(
            str(result.object_id), str(result.version_id)
        )
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        replica.status = ReplicaState.UNAVAILABLE

        reconciler = PartitionReconciler(db_session, clients)
        state = await reconciler.reconcile_version(result.version_id)
        assert state.unavailable == 1
        assert state.healthy == 2

        worker = RepairWorker(db_session, coordinator)
        run = await worker.run_once()

        assert run.scheduled == 1
        assert run.succeeded == 1

        final = await reconciler.reconcile_version(result.version_id)
        assert final.healthy == 3
        assert final.unavailable == 0
        assert await coordinator.read_object("missing.bin") == b"missing-data"


@pytest.mark.asyncio
async def test_reconciliation_keeps_historical_versions_independent(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients, _transports = await build_nodes(tmp_path, stack)
        register(db_session, clients)

        coordinator = DistributedWriteCoordinator(
            db_session, clients, replication_factor=3, write_quorum=2, read_quorum=1
        )
        first = await coordinator.write_object("versions.bin", b"version-one")
        second = await coordinator.write_object("versions.bin", b"version-two")

        reconciler = PartitionReconciler(db_session, clients)

        first_result = await reconciler.reconcile_version(first.version_id)
        second_result = await reconciler.reconcile_version(second.version_id)

        assert first_result.healthy == 3
        assert second_result.healthy == 3
        assert first.version_id != second.version_id
        assert await coordinator.read_object("versions.bin") == b"version-two"

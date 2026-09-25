from __future__ import annotations

from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import ReplicaState
from metadata.manager import MetadataManager
from metadata.models import Replica
from replication.coordinator import DistributedWriteCoordinator
from replication.reconciler import PartitionReconciler
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
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


async def nodes(tmp_path: Path, stack: AsyncExitStack):
    clients, transports = {}, {}
    for node_id in ("node-1", "node-2", "node-3"):
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
        clients, transports = await nodes(tmp_path, stack)
        register(db_session, clients)

        coordinator = DistributedWriteCoordinator(
            db_session, clients, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("reconcile.bin", b"canonical-data")

        # Corrupt node-1 after the successful write. The replica remains
        # self-consistent as storage, but disagrees with the authoritative
        # metadata checksum.
        engine = StorageEngine(
            tmp_path / "node-1", capacity_bytes=10_000_000, chunk_size_bytes=4
        )
        await engine.write_stream(
            str(result.object_id),
            str(result.version_id),
            _chunks(b"divergent-data"),
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

        # Reconciliation never copies the divergent bytes into another replica.
        assert await coordinator.read_object("reconcile.bin") == b"canonical-data"
        assert await clients["node-2"].verify_object(
            str(result.object_id), str(result.version_id)
        )

        # The node remains live; only the replica is bad.
        assert not transports["node-1"].down


@pytest.mark.asyncio
async def test_reconciliation_marks_partitioned_replica_unavailable_and_recovers_after_heal(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients, transports = await nodes(tmp_path, stack)
        register(db_session, clients)

        coordinator = DistributedWriteCoordinator(
            db_session, clients, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("partition.bin", b"partition-data")

        transports["node-1"].down = True
        reconciler = PartitionReconciler(db_session, clients)

        during_partition = await reconciler.reconcile_version(result.version_id)
        assert during_partition.unavailable == 1
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        assert replica.status is ReplicaState.UNAVAILABLE

        transports["node-1"].down = False
        # The detector is responsible for node-state recovery; reconciliation
        # then validates the bytes once the node is healthy again.
        from health.failure_detector import FailureDetector
        from common.constants import NodeState
        detector = FailureDetector(db_session, clients)
        assert (await detector.probe_node("node-1")).state is NodeState.RECOVERING
        assert (await detector.probe_node("node-1")).state is NodeState.HEALTHY

        # A recovered replica must be repaired rather than silently revived.
        from repair.worker import RepairWorker
        worker = __import__("repair.worker", fromlist=["RepairWorker"]).RepairWorker(
            db_session, coordinator
        )
        run = await worker.run_once()
        assert run.succeeded == 1

        repaired = await reconciler.reconcile_version(result.version_id)
        assert repaired.healthy == 3
        assert repaired.unavailable == 0


@pytest.mark.asyncio
async def test_reconciliation_does_not_treat_old_committed_version_as_divergence(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        clients, _transports = await nodes(tmp_path, stack)
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


async def _chunks(data: bytes):
    yield data

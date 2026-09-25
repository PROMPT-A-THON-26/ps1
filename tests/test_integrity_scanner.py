from __future__ import annotations

from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import NodeState, ReplicaState
from health.failure_detector import FailureDetector
from integrity.scanner import IntegrityScanner
from metadata.manager import MetadataManager
from metadata.models import Replica
from repair.worker import RepairWorker
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine
from worker.reliability_service import ReliabilityService


class ToggleTransport(httpx.AsyncBaseTransport):
    def __init__(self, app) -> None:
        self.inner = httpx.ASGITransport(app=app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await self.inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self.inner.aclose()


async def build_nodes(tmp_path: Path, stack: AsyncExitStack):
    nodes = {}
    for node_id in ("node-1", "node-2", "node-3", "node-4"):
        engine = StorageEngine(
            tmp_path / node_id,
            capacity_bytes=10_000_000,
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


def register_nodes(session, nodes) -> None:
    manager = MetadataManager(session)
    for node_id in nodes:
        manager.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=10_000_000,
            status=NodeState.HEALTHY,
        )


@pytest.mark.asyncio
async def test_integrity_scanner_detects_divergent_replica_without_promoting_it(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        nodes = await build_nodes(tmp_path, stack)
        register_nodes(db_session, nodes)
        coordinator = DistributedWriteCoordinator(
            db_session, nodes, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("integrity.bin", b"integrity-data")

        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None

        object_dir = tmp_path / "node-1" / "objects" / str(result.object_id) / str(result.version_id)
        chunk = object_dir / "chunk-000000"
        chunk.write_bytes(b"CORRUPTED")

        scanner = IntegrityScanner(db_session, nodes)
        scan = await scanner.scan_once()

        assert scan.versions_scanned == 1
        assert scan.replicas_checked == 3
        assert scan.corrupted == 1
        assert scan.healthy == 2

        db_session.expire_all()
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        assert replica.status is ReplicaState.CORRUPTED
        assert (await coordinator.read_object("integrity.bin")) == b"integrity-data"


@pytest.mark.asyncio
async def test_reliability_service_scans_corruption_then_repairs_it(db_session, tmp_path):
    async with AsyncExitStack() as stack:
        nodes = await build_nodes(tmp_path, stack)
        register_nodes(db_session, nodes)
        coordinator = DistributedWriteCoordinator(
            db_session, nodes, replication_factor=3, write_quorum=2, read_quorum=1
        )
        result = await coordinator.write_object("auto-integrity.bin", b"auto-repair")

        object_dir = tmp_path / "node-1" / "objects" / str(result.object_id) / str(result.version_id)
        (object_dir / "chunk-000000").write_bytes(b"BAD")

        detector = FailureDetector(db_session, nodes)
        scanner = IntegrityScanner(db_session, nodes)
        worker = RepairWorker(db_session, coordinator)
        service = ReliabilityService(detector, worker, integrity_scanner=scanner)

        _, scan, repair = await service.run_once()

        assert scan is not None
        assert scan.corrupted == 1
        assert repair.scheduled == 1
        assert repair.succeeded == 1
        assert repair.failed == 0
        assert await coordinator.read_object("auto-integrity.bin") == b"auto-repair"

        db_session.expire_all()
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == result.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        assert replica.status is ReplicaState.CORRUPTED

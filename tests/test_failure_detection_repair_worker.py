from __future__ import annotations

from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select

from common.constants import JobStatus, NodeState, ReplicaState
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, StorageNode
from health.failure_detector import FailureDetector, FailureDetectorConfig
from repair.worker import RepairWorker
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


class ToggleTransport(httpx.AsyncBaseTransport):
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
        app = create_storage_node_app(engine, node_id, NodeLifecycle())
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


def register_nodes(session, nodes) -> None:
    manager = MetadataManager(session)
    for node_id in nodes:
        manager.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=10_000_000,
        )


@pytest.mark.asyncio
async def test_failure_detector_progresses_suspect_to_unavailable_and_marks_replicas(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        nodes, transports = await build_nodes(tmp_path, stack)
        register_nodes(db_session, nodes)

        coordinator = DistributedWriteCoordinator(
            db_session,
            nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )
        write = await coordinator.write_object("detector.bin", b"detector-test")

        base = datetime.now(timezone.utc) + timedelta(seconds=60)
        clock = [base]
        detector = FailureDetector(
            db_session,
            nodes,
            config=FailureDetectorConfig(
                interval_seconds=1,
                suspect_after_seconds=10,
                unavailable_after_seconds=20,
            ),
            now=lambda: clock[0],
        )

        first = await detector.probe_node("node-1")
        assert first.state is NodeState.HEALTHY

        transports["node-1"].down = True
        clock[0] = base + timedelta(seconds=15)
        suspect = await detector.probe_node("node-1")
        assert suspect.state is NodeState.SUSPECT
        assert suspect.reachable is False

        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == write.version_id,
                Replica.node_id == "node-1",
            )
        )
        assert replica is not None
        assert replica.status is ReplicaState.HEALTHY

        clock[0] = base + timedelta(seconds=25)
        unavailable = await detector.probe_node("node-1")
        assert unavailable.state is NodeState.UNAVAILABLE
        assert unavailable.replica_count_marked_unavailable == 1

        db_session.expire_all()
        replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == write.version_id,
                Replica.node_id == "node-1",
            )
        )
        node = db_session.scalar(
            select(StorageNode).where(StorageNode.node_id == "node-1")
        )
        assert replica is not None and replica.status is ReplicaState.UNAVAILABLE
        assert node is not None and node.status is NodeState.UNAVAILABLE
        assert node.last_heartbeat_at == base


@pytest.mark.asyncio
async def test_failure_detector_recovers_unavailable_node_in_two_successful_probes(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        nodes, transports = await build_nodes(tmp_path, stack)
        register_nodes(db_session, nodes)

        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        clock = [base]
        detector = FailureDetector(
            db_session,
            nodes,
            config=FailureDetectorConfig(
                interval_seconds=1,
                suspect_after_seconds=10,
                unavailable_after_seconds=20,
            ),
            now=lambda: clock[0],
        )

        assert (await detector.probe_node("node-1")).state is NodeState.HEALTHY

        transports["node-1"].down = True
        clock[0] = base + timedelta(seconds=25)
        assert (await detector.probe_node("node-1")).state is NodeState.UNAVAILABLE

        transports["node-1"].down = False
        clock[0] = base + timedelta(seconds=26)
        recovering = await detector.probe_node("node-1")
        assert recovering.state is NodeState.RECOVERING

        clock[0] = base + timedelta(seconds=27)
        healthy = await detector.probe_node("node-1")
        assert healthy.state is NodeState.HEALTHY


@pytest.mark.asyncio
async def test_repair_worker_automatically_restores_replication_factor(
    db_session, tmp_path
):
    async with AsyncExitStack() as stack:
        nodes, transports = await build_nodes(tmp_path, stack)
        register_nodes(db_session, nodes)

        coordinator = DistributedWriteCoordinator(
            db_session,
            nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )
        write = await coordinator.write_object("worker.bin", b"background-repair")

        base = datetime(2026, 1, 1, tzinfo=timezone.utc)
        detector = FailureDetector(
            db_session,
            nodes,
            config=FailureDetectorConfig(
                interval_seconds=1,
                suspect_after_seconds=10,
                unavailable_after_seconds=20,
            ),
            now=lambda: base + timedelta(seconds=25),
        )

        transports["node-1"].down = True
        result = await detector.probe_node("node-1")
        assert result.state is NodeState.UNAVAILABLE

        worker = RepairWorker(
            db_session,
            coordinator,
        )
        run = await worker.run_once()

        assert run.scheduled == 1
        assert run.succeeded == 1
        assert run.failed == 0

        jobs = list(
            db_session.scalars(
                select(RepairJob).where(RepairJob.version_id == write.version_id)
            )
        )
        assert len(jobs) == 1
        assert jobs[0].status is JobStatus.SUCCEEDED
        assert jobs[0].failed_node_id == "node-1"
        assert jobs[0].target_node_id == "node-4"

        replicas = list(
            db_session.scalars(
                select(Replica).where(Replica.version_id == write.version_id)
            )
        )
        healthy_nodes = {
            replica.node_id
            for replica in replicas
            if replica.status is ReplicaState.HEALTHY
        }
        assert healthy_nodes == {"node-2", "node-3", "node-4"}
        assert await coordinator.read_object("worker.bin") == b"background-repair"


@pytest.mark.asyncio
async def test_repair_worker_does_not_duplicate_active_jobs(db_session, tmp_path):
    async with AsyncExitStack() as stack:
        nodes, transports = await build_nodes(tmp_path, stack)
        register_nodes(db_session, nodes)

        coordinator = DistributedWriteCoordinator(
            db_session,
            nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )
        write = await coordinator.write_object("dedupe.bin", b"dedupe")
        transports["node-1"].down = True

        manager = MetadataManager(db_session)
        manager.mark_node_replicas_unavailable("node-1")

        worker = RepairWorker(db_session, coordinator)
        first = await worker._schedule_missing_repairs()
        second = await worker._schedule_missing_repairs()

        assert first.scheduled == 1
        assert second.scheduled == 0

        jobs = list(
            db_session.scalars(
                select(RepairJob).where(RepairJob.version_id == write.version_id)
            )
        )
        assert len(jobs) == 1
        assert jobs[0].status is JobStatus.PENDING
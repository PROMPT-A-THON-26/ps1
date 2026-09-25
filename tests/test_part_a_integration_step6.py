"""Step 6 integration tests against the real Part-A storage-node implementation."""

from __future__ import annotations

from contextlib import AsyncExitStack

import httpx
import pytest
from sqlalchemy import select

from common.constants import NodeState, ReplicaState
from metadata.manager import MetadataManager
from metadata.models import Replica
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import (
    RetryPolicy,
    StorageNodeClient,
    StorageNodeClientConfig,
    StorageNodeUnavailableError,
)
from repair.worker import RepairWorker
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


class FailingHealthClient:
    """Wrap a real storage client but simulate a control-plane node outage."""

    def __init__(self, inner: StorageNodeClient) -> None:
        self.inner = inner

    async def health(self):
        raise StorageNodeUnavailableError("simulated node failure")

    async def stats(self):
        raise StorageNodeUnavailableError("simulated node failure")

    async def verify_object(self, *args, **kwargs):
        return await self.inner.verify_object(*args, **kwargs)

    def stream_object(self, *args, **kwargs):
        return self.inner.stream_object(*args, **kwargs)

    async def put_object(self, *args, **kwargs):
        return await self.inner.put_object(*args, **kwargs)

    async def delete_object(self, *args, **kwargs):
        return await self.inner.delete_object(*args, **kwargs)

    async def aclose(self) -> None:
        await self.inner.aclose()


async def build_real_nodes(tmp_path, stack: AsyncExitStack):
    nodes = {}
    for node_id in ("node-1", "node-2", "node-3", "node-4"):
        engine = StorageEngine(
            tmp_path / node_id,
            capacity_bytes=1024 * 1024,
            chunk_size_bytes=4096,
        )
        app = create_storage_node_app(engine, node_id, NodeLifecycle())
        transport = httpx.ASGITransport(app=app)
        client_transport = await stack.enter_async_context(
            httpx.AsyncClient(
                transport=transport,
                base_url=f"http://{node_id}:9001",
            )
        )
        client = StorageNodeClient(
            StorageNodeClientConfig(
                f"http://{node_id}:9001",
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
async def test_part_a_storage_contract_and_distributed_repair(db_session, tmp_path):
    payload = (b"part-a-integration-" * 256) + b"done"

    async with AsyncExitStack() as stack:
        raw_nodes = await build_real_nodes(tmp_path, stack)
        metadata = MetadataManager(db_session)

        for node_id in raw_nodes:
            metadata.register_node(
                node_id=node_id,
                address=f"http://{node_id}:9001",
                capacity_bytes=1024 * 1024,
                status=NodeState.HEALTHY,
            )

        # Validate the real Part-A HTTP contract on every node before control
        # plane orchestration starts.
        for node_id, client in raw_nodes.items():
            health = await client.health()
            assert health.node_id == node_id
            assert health.status == "healthy"

        coordinator = DistributedWriteCoordinator(
            db_session,
            raw_nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )

        written = await coordinator.write_object("integration.bin", payload)
        assert len(written.healthy_nodes) == 3
        assert written.failed_nodes == ()

        read_back = await coordinator.read_object("integration.bin")
        assert read_back == payload

        failed_replica = db_session.scalar(
            select(Replica).where(
                Replica.version_id == written.version_id,
                Replica.node_id == written.healthy_nodes[1],
            )
        )
        assert failed_replica is not None

        metadata.set_replica_state(
            failed_replica.replica_id,
            ReplicaState.UNAVAILABLE,
        )
        metadata.set_node_status(
            failed_replica.node_id,
            NodeState.UNAVAILABLE,
        )

        # Repair using the real Part-A storage implementations and a real
        # streaming GET -> PUT path. Only the selected node is simulated as
        # unavailable; the replacement target is another real storage app.
        failed_node_id = failed_replica.node_id
        failed_inner = raw_nodes[failed_node_id]
        test_nodes = dict(raw_nodes)
        test_nodes[failed_node_id] = FailingHealthClient(failed_inner)

        coordinator_after_failure = DistributedWriteCoordinator(
            db_session,
            test_nodes,
            replication_factor=3,
            write_quorum=2,
            read_quorum=1,
        )
        worker = RepairWorker(db_session, coordinator_after_failure)

        repair_result = await worker.run_once()
        assert repair_result.scheduled == 1
        assert repair_result.succeeded == 1
        assert repair_result.failed == 0

        healthy = list(
            db_session.scalars(
                select(Replica).where(
                    Replica.version_id == written.version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
            )
        )
        assert len(healthy) == 3

        # The object remains readable from the surviving real storage nodes.
        assert await coordinator_after_failure.read_object("integration.bin") == payload

        # Verify every remaining healthy replica directly through Part A.
        for replica in healthy:
            verified = await raw_nodes[replica.node_id].verify_object(
                str(written.object_id),
                str(written.version_id),
            )
            assert verified.valid is True
            assert verified.size_bytes == len(payload)
            assert verified.checksum == written.checksum

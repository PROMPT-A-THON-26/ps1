"""B14 integration tests against the actual Part A storage-node implementation."""

from __future__ import annotations

from hashlib import sha256

import httpx
import pytest
from httpx import ASGITransport

from common.constants import NodeState
from gateway.service import GatewayService
from metadata.manager import MetadataManager
from replication import ReplicationPolicy
from replication.node_client import (
    StorageNodeClient,
    StorageObjectAlreadyExistsError,
    StorageObjectNotFoundError,
)
from storage import node_server
from storage.storage_engine import StorageEngine


@pytest.mark.asyncio
async def test_storage_node_client_matches_part_a_http_contract(tmp_path):
    node_server.engine = StorageEngine(
        tmp_path,
        capacity_bytes=1024 * 1024,
        chunk_size_bytes=4,
    )

    transport = ASGITransport(app=node_server.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://node-01:9001",
    ) as http_client:
        client = StorageNodeClient(
            "http://node-01:9001",
            client=http_client,
        )

        health = await client.health()
        assert health.node_id == node_server.config.node_id
        assert health.status == "healthy"

        stats = await client.stats()
        assert stats.capacity_bytes == 1024 * 1024
        assert stats.used_bytes == 0
        assert stats.free_bytes == 1024 * 1024

        request_id = "req-part-a-contract"
        stored = await client.put_object(
            "object-1",
            "version-1",
            b"abcdefgh",
            request_id=request_id,
        )
        assert stored.object_id == "object-1"
        assert stored.version_id == "version-1"
        assert stored.size_bytes == 8

        with pytest.raises(StorageObjectAlreadyExistsError):
            await client.put_object(
                "object-1",
                "version-1",
                b"duplicate",
            )

        assert await client.head_object("object-1", "version-1") == 8

        async with client.stream_object("object-1", "version-1") as response:
            body = b"".join([chunk async for chunk in response.aiter_bytes()])
        assert body == b"abcdefgh"

        verified = await client.verify_object(
            "object-1",
            "version-1",
            request_id=request_id,
        )
        assert verified.verified is True
        assert verified.valid is True
        assert verified.size_bytes == 8
        assert verified.checksum == sha256(b"abcdefgh").hexdigest()

        await client.delete_object("object-1", "version-1")

        with pytest.raises(StorageObjectNotFoundError):
            await client.head_object("object-1", "version-1")


@pytest.mark.asyncio
async def test_gateway_end_to_end_replicates_through_real_part_a_node(
    db_session,
    tmp_path,
):
    node_server.engine = StorageEngine(
        tmp_path,
        capacity_bytes=1024 * 1024,
        chunk_size_bytes=4,
    )

    metadata = MetadataManager(db_session)
    metadata.register_node(
        node_id=node_server.config.node_id,
        address="http://node-01:9001",
        capacity_bytes=1024 * 1024,
        status=NodeState.HEALTHY,
    )

    transport = ASGITransport(app=node_server.app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://node-01:9001",
    ) as http_client:

        def client_factory(address: str) -> StorageNodeClient:
            return StorageNodeClient(address, client=http_client)

        gateway = GatewayService(db_session)

        async def payload():
            yield b"vault-"
            yield b"integration-"
            yield b"payload"

        result = await gateway.put_object(
            "integration.txt",
            payload(),
            expected_current_version=0,
            replication_policy=ReplicationPolicy(
                factor=1,
                write_quorum=1,
                read_quorum=1,
            ),
            client_factory=client_factory,
        )

        assert result["version_number"] == 1
        assert result["replicas"] == [node_server.config.node_id]

        object_id = result["object_id"]
        version_id = result["version_id"]
        assert node_server.engine.exists(object_id, version_id)

        verify = await client_factory("http://node-01:9001").verify_object(
            object_id,
            version_id,
        )
        assert verify.verified is True
        assert verify.valid is True
        assert verify.checksum == result["checksum"]
        assert verify.size_bytes == result["size_bytes"]

        obj, version, targets = gateway.read_targets("integration.txt")
        assert obj.object_id.hex
        selected, remaining = await gateway.preflight_read_target(
            targets,
            object_id=str(obj.object_id),
            version_id=str(version.version_id),
            client_factory=client_factory,
        )
        assert selected.node_id == node_server.config.node_id
        assert [node.node_id for node in remaining] == [node_server.config.node_id]

        async with client_factory(selected.address).stream_object(
            str(obj.object_id),
            str(version.version_id),
        ) as response:
            downloaded = b"".join(
                [chunk async for chunk in response.aiter_bytes()]
            )

        expected_payload = b"vault-integration-payload"
        assert downloaded == expected_payload

        deletion = await gateway.delete_object(
            "integration.txt",
            client_factory=client_factory,
        )
        assert deletion["state"] == "DELETED"
        assert not node_server.engine.exists(object_id, version_id)

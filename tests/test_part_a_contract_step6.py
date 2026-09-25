"""Additional Step 6 regression coverage for the complete Part-A node contract."""

from __future__ import annotations

import httpx
import pytest
from httpx import ASGITransport
from hashlib import sha256

from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientConfig,
    StorageObjectNotFoundError,
)
from storage.app_factory import create_storage_node_app
from storage.node_lifecycle import NodeLifecycle
from storage.storage_engine import StorageEngine


@pytest.mark.asyncio
async def test_step6_real_part_a_client_contract(tmp_path):
    engine = StorageEngine(
        tmp_path / "node-contract",
        capacity_bytes=1024 * 1024,
        chunk_size_bytes=4,
    )
    app = create_storage_node_app(engine, "node-contract", NodeLifecycle())

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://node-contract:9001",
    ) as transport:
        client = StorageNodeClient(
            StorageNodeClientConfig("http://node-contract:9001"),
            client=transport,
        )

        payload = b"step6-real-part-a-contract"
        stored = await client.put_object("object-contract", "version-1", payload)
        assert stored.size_bytes == len(payload)

        assert await client.head_object("object-contract", "version-1") == len(payload)

        async with client.stream_object("object-contract", "version-1") as response:
            downloaded = b"".join([chunk async for chunk in response.aiter_bytes()])
        assert downloaded == payload

        verified = await client.verify_object("object-contract", "version-1")
        assert verified.valid is True
        assert verified.verified is True
        assert verified.size_bytes == len(payload)
        assert verified.checksum == sha256(payload).hexdigest()

        await client.delete_object("object-contract", "version-1")

        with pytest.raises(StorageObjectNotFoundError):
            await client.head_object("object-contract", "version-1")

from __future__ import annotations

from hashlib import sha256

import httpx
import pytest
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport

from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientConfig,
    StorageNodeInsufficientCapacityError,
    StorageNodeUnavailableError,
    StorageObjectAlreadyExistsError,
    StorageObjectNotFoundError,
    StorageNodeProtocolError,
)


def build_mock_node() -> tuple[FastAPI, dict[tuple[str, str], bytes]]:
    app = FastAPI()
    objects: dict[tuple[str, str], bytes] = {}

    @app.get("/internal/v1/health")
    async def health() -> dict[str, str]:
        return {"status": "healthy", "node_id": "node-test"}

    @app.get("/internal/v1/stats")
    async def stats() -> dict[str, int | str]:
        used = sum(len(value) for value in objects.values())
        capacity = 10_000
        return {
            "node_id": "node-test",
            "capacity_bytes": capacity,
            "used_bytes": used,
            "free_bytes": capacity - used,
        }

    @app.put("/internal/v1/objects/{object_id}/{version_id}")
    async def put_object(object_id: str, version_id: str, request: Request) -> Response:
        key = (object_id, version_id)
        if key == ("capacity", "version"):
            return Response(status_code=507, content=b"full")
        if key in objects:
            return Response(status_code=409, content=b"exists")
        data = b"".join([chunk async for chunk in request.stream()])
        objects[key] = data
        return Response(
            status_code=201,
            headers={"content-type": "application/json"},
            content=(
                '{"object_id":"%s","version_id":"%s","size_bytes":%d}'
                % (object_id, version_id, len(data))
            ).encode(),
        )

    @app.get("/internal/v1/objects/{object_id}/{version_id}")
    async def get_object(object_id: str, version_id: str) -> Response:
        data = objects.get((object_id, version_id))
        if data is None:
            return Response(status_code=404, content=b"missing")
        return Response(content=data, media_type="application/octet-stream")

    @app.head("/internal/v1/objects/{object_id}/{version_id}")
    async def head_object(object_id: str, version_id: str) -> Response:
        data = objects.get((object_id, version_id))
        if data is None:
            return Response(status_code=404, content=b"missing")
        return Response(headers={"content-length": str(len(data))})

    @app.delete("/internal/v1/objects/{object_id}/{version_id}")
    async def delete_object(object_id: str, version_id: str) -> Response:
        if (object_id, version_id) not in objects:
            return Response(status_code=404, content=b"missing")
        del objects[(object_id, version_id)]
        return Response(status_code=204)

    @app.get("/internal/v1/objects/{object_id}/{version_id}/verify")
    async def verify_object(object_id: str, version_id: str) -> Response | dict[str, object]:
        data = objects.get((object_id, version_id))
        if data is None:
            return Response(status_code=404, content=b"missing")
        return {
            "object_id": object_id,
            "version_id": version_id,
            "size_bytes": len(data),
            "checksum": sha256(data).hexdigest(),
            "verified": True,
        }

    return app, objects


@pytest.mark.asyncio
async def test_storage_node_client_full_contract() -> None:
    app, objects = build_mock_node()
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        async with StorageNodeClient(
            StorageNodeClientConfig("http://testserver"),
            client=transport_client,
        ) as node:
            payload = b"large-enough-for-streaming-contract"
            uploaded = await node.put_object(
                "obj-1",
                "ver-1",
                payload,
                request_id="req-test",
            )
            assert uploaded.size_bytes == len(payload)

            assert await node.head_object("obj-1", "ver-1") == len(payload)

            async with node.stream_object("obj-1", "ver-1") as response:
                received = b"".join([chunk async for chunk in response.aiter_bytes()])
            assert received == payload

            verified = await node.verify_object("obj-1", "ver-1")
            assert verified.verified is True
            assert verified.size_bytes == len(payload)
            assert verified.checksum == sha256(payload).hexdigest()

            health = await node.health()
            assert health.node_id == "node-test"
            assert health.status == "healthy"

            stats = await node.stats()
            assert stats.used_bytes == len(payload)
            assert stats.free_bytes == stats.capacity_bytes - len(payload)

            await node.delete_object("obj-1", "ver-1")
            assert ("obj-1", "ver-1") not in objects


@pytest.mark.asyncio
async def test_streaming_upload_accepts_async_iterable() -> None:
    app, objects = build_mock_node()
    chunks = [b"chunk-1", b"chunk-2", b"chunk-3"]

    async def source():
        for chunk in chunks:
            yield chunk

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        async with StorageNodeClient("http://testserver", client=transport_client) as node:
            result = await node.put_object("obj", "ver", source())
            assert result.size_bytes == sum(map(len, chunks))
            assert objects[("obj", "ver")] == b"".join(chunks)


@pytest.mark.asyncio
async def test_storage_node_error_mapping() -> None:
    app = FastAPI()

    @app.get("/internal/v1/objects/{object_id}/{version_id}")
    async def missing(_: str, __: str) -> Response:
        return Response(status_code=404, content=b"missing")

    @app.put("/internal/v1/objects/{object_id}/{version_id}")
    async def existing(_: str, __: str, request: Request) -> Response:
        del request
        return Response(status_code=409, content=b"exists")

    @app.put("/internal/v1/objects/capacity/version")
    async def capacity(request: Request) -> Response:
        del request
        return Response(status_code=507, content=b"full")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        async with StorageNodeClient("http://testserver", client=transport_client) as node:
            with pytest.raises(StorageObjectNotFoundError):
                async with node.stream_object("missing", "ver"):
                    pass

            with pytest.raises(StorageObjectAlreadyExistsError):
                await node.put_object("existing", "ver", b"x")

            with pytest.raises(StorageNodeInsufficientCapacityError):
                await node.put_object("capacity", "version", b"x")


@pytest.mark.asyncio
async def test_protocol_error_on_success_with_malformed_payload() -> None:
    app = FastAPI()

    @app.put("/internal/v1/objects/{object_id}/{version_id}")
    async def malformed(_: str, __: str, request: Request) -> Response:
        del request
        return Response(
            status_code=201,
            content=b'{"object_id":"wrong","version_id":"wrong","size_bytes":1}',
            media_type="application/json",
        )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        async with StorageNodeClient("http://testserver", client=transport_client) as node:
            with pytest.raises(StorageNodeProtocolError):
                await node.put_object("obj", "ver", b"x")


@pytest.mark.asyncio
async def test_unreachable_node_is_explicit() -> None:
    async with StorageNodeClient("http://127.0.0.1:1", timeout_seconds=0.05) as node:
        with pytest.raises(StorageNodeUnavailableError):
            await node.health()


def test_client_rejects_path_traversal_identifiers() -> None:
    with pytest.raises(ValueError):
        StorageNodeClient("http://test")._object_path("..", "version")

    with pytest.raises(ValueError):
        StorageNodeClient("http://test")._object_path("object", "version/evil")


def test_client_config_validates_address_and_timeout() -> None:
    with pytest.raises(ValueError):
        StorageNodeClientConfig(" ", 1)
    with pytest.raises(ValueError):
        StorageNodeClientConfig("http://test", 0)

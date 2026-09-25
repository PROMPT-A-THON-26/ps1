from __future__ import annotations

from hashlib import sha256

import httpx
import pytest
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport

from replication.node_client import (
    RetryPolicy,
    StorageNodeClient,
    StorageNodeClientConfig,
    StorageNodeInsufficientCapacityError,
    StorageNodeTimeouts,
    StorageNodeUnavailableError,
    StorageObjectAlreadyExistsError,
    StorageObjectNotFoundError,
    StorageNodeInvalidRequestError,
    StorageNodeIntegrityError,
    StorageNodeProtocolError,
)


def build_mock_node() -> tuple[FastAPI, dict[tuple[str, str], bytes], dict[str, list[str]]]:
    app = FastAPI()
    objects: dict[tuple[str, str], bytes] = {}
    seen_request_ids: dict[str, list[str]] = {"put": [], "get": [], "head": [], "delete": [], "verify": [], "health": [], "stats": []}

    @app.get("/internal/v1/health")
    async def health(request: Request) -> dict[str, str]:
        seen_request_ids["health"].append(request.headers.get("x-request-id", ""))
        return {"status": "healthy", "node_id": "node-test"}

    @app.get("/internal/v1/stats")
    async def stats(request: Request) -> dict[str, int | str]:
        seen_request_ids["stats"].append(request.headers.get("x-request-id", ""))
        used = sum(len(value) for value in objects.values())
        capacity = 10_000
        return {
            "node_id": "node-test",
            "capacity_bytes": capacity,
            "used_bytes": used,
            "free_bytes": capacity - used,
        }

    # Put the special test route before the parameterized route. FastAPI uses
    # route registration order for matching.
    @app.put("/internal/v1/objects/{object_id}/{version_id}")
    async def put_object(object_id: str, version_id: str, request: Request) -> Response:
        seen_request_ids["put"].append(request.headers.get("x-request-id", ""))
        key = (object_id, version_id)
        if key == ("capacity", "version"):
            return Response(status_code=507, content=b"full")
        if key == ("existing", "ver"):
            return Response(status_code=409, content=b"exists")
        if key == ("invalid", "request"):
            return Response(status_code=400, content=b"invalid")
        if key in objects:
            return Response(status_code=409, content=b"exists")
        data = b"".join([chunk async for chunk in request.stream()])
        objects[key] = data
        return Response(
            status_code=201,
            content=(
                '{"object_id":"%s","version_id":"%s","size_bytes":%d}'
                % (object_id, version_id, len(data))
            ).encode(),
            media_type="application/json",
        )

    @app.get("/internal/v1/objects/{object_id}/{version_id}")
    async def get_object(object_id: str, version_id: str, request: Request) -> Response:
        seen_request_ids["get"].append(request.headers.get("x-request-id", ""))
        data = objects.get((object_id, version_id))
        if data is None:
            return Response(status_code=404, content=b"missing")
        return Response(content=data, media_type="application/octet-stream")

    @app.head("/internal/v1/objects/{object_id}/{version_id}")
    async def head_object(object_id: str, version_id: str, request: Request) -> Response:
        seen_request_ids["head"].append(request.headers.get("x-request-id", ""))
        data = objects.get((object_id, version_id))
        if data is None:
            return Response(status_code=404, content=b"missing")
        return Response(headers={"content-length": str(len(data))})

    @app.delete("/internal/v1/objects/{object_id}/{version_id}")
    async def delete_object(object_id: str, version_id: str, request: Request) -> Response:
        seen_request_ids["delete"].append(request.headers.get("x-request-id", ""))
        if (object_id, version_id) not in objects:
            return Response(status_code=404, content=b"missing")
        del objects[(object_id, version_id)]
        return Response(status_code=204)

    @app.get("/internal/v1/objects/{object_id}/{version_id}/verify")
    async def verify_object(object_id: str, version_id: str, request: Request) -> dict[str, object]:
        seen_request_ids["verify"].append(request.headers.get("x-request-id", ""))
        data = objects.get((object_id, version_id))
        if data is None:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="missing")
        return {
            "object_id": object_id,
            "version_id": version_id,
            "size_bytes": len(data),
            "checksum": sha256(data).hexdigest(),
            "verified": True,
        }

    return app, objects, seen_request_ids


@pytest.fixture
def no_delay_retry_policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=3, base_delay_seconds=0, max_delay_seconds=0, jitter_ratio=0)


@pytest.mark.asyncio
async def test_storage_node_client_full_contract(no_delay_retry_policy: RetryPolicy) -> None:
    app, objects, seen = build_mock_node()
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig(
            "http://testserver",
            retry_policy=no_delay_retry_policy,
        )
        async with StorageNodeClient(config, client=transport_client) as node:
            payload = b"large-enough-for-streaming-contract"
            uploaded = await node.put_object("obj-1", "ver-1", payload, request_id="req-test")
            assert uploaded.size_bytes == len(payload)
            assert seen["put"] == ["req-test"]

            assert await node.head_object("obj-1", "ver-1", request_id="req-test-head") == len(payload)

            async with node.stream_object("obj-1", "ver-1", request_id="req-test-get") as response:
                received = b"".join([chunk async for chunk in response.aiter_bytes()])
            assert received == payload

            verified = await node.verify_object("obj-1", "ver-1", request_id="req-test-verify")
            assert verified.verified is True
            assert verified.valid is True
            assert verified.size_bytes == len(payload)
            assert verified.checksum == sha256(payload).hexdigest()

            health = await node.health(request_id="req-test-health")
            assert health.node_id == "node-test"
            assert health.status == "healthy"

            stats = await node.stats(request_id="req-test-stats")
            assert stats.used_bytes == len(payload)

            await node.delete_object("obj-1", "ver-1", request_id="req-test-delete")
            assert ("obj-1", "ver-1") not in objects

    assert seen["head"] == ["req-test-head"]
    assert seen["get"] == ["req-test-get"]
    assert seen["verify"] == ["req-test-verify"]
    assert seen["health"] == ["req-test-health"]
    assert seen["stats"] == ["req-test-stats"]
    assert seen["delete"] == ["req-test-delete"]


@pytest.mark.asyncio
async def test_streaming_upload_accepts_async_iterable(no_delay_retry_policy: RetryPolicy) -> None:
    app, objects, _ = build_mock_node()
    chunks = [b"chunk-1", b"chunk-2", b"chunk-3"]

    async def source():
        for chunk in chunks:
            yield chunk

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        async with StorageNodeClient(
            "http://testserver",
            client=transport_client,
        ) as node:
            node.config = StorageNodeClientConfig(
                "http://testserver",
                retry_policy=no_delay_retry_policy,
            )
            result = await node.put_object("obj", "ver", source())
            assert result.size_bytes == sum(map(len, chunks))
            assert objects[("obj", "ver")] == b"".join(chunks)


@pytest.mark.asyncio
async def test_storage_node_error_mapping(no_delay_retry_policy: RetryPolicy) -> None:
    app, _, _ = build_mock_node()
    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig("http://testserver", retry_policy=no_delay_retry_policy)
        async with StorageNodeClient(config, client=transport_client) as node:
            with pytest.raises(StorageObjectNotFoundError):
                async with node.stream_object("missing", "ver"):
                    pass

            with pytest.raises(StorageObjectAlreadyExistsError):
                await node.put_object("existing", "ver", b"x")

            with pytest.raises(StorageNodeInsufficientCapacityError):
                await node.put_object("capacity", "version", b"x")

            with pytest.raises(StorageNodeInvalidRequestError):
                await node.put_object("invalid", "request", b"x")


@pytest.mark.asyncio
async def test_protocol_error_on_success_with_malformed_payload(no_delay_retry_policy: RetryPolicy) -> None:
    app = FastAPI()

    @app.put("/internal/v1/objects/{object_id}/{version_id}")
    async def malformed(object_id: str, version_id: str, request: Request) -> Response:
        del object_id, version_id, request
        return Response(
            status_code=201,
            content=b'{"object_id":"wrong","version_id":"wrong","size_bytes":1}',
            media_type="application/json",
        )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig("http://testserver", retry_policy=no_delay_retry_policy)
        async with StorageNodeClient(config, client=transport_client) as node:
            with pytest.raises(StorageNodeProtocolError):
                await node.put_object("obj", "ver", b"x")


@pytest.mark.asyncio
async def test_verify_surfaces_reported_corruption(no_delay_retry_policy: RetryPolicy) -> None:
    app = FastAPI()

    @app.get("/internal/v1/objects/{object_id}/{version_id}/verify")
    async def verify(object_id: str, version_id: str) -> dict[str, object]:
        return {
            "object_id": object_id,
            "version_id": version_id,
            "size_bytes": 4,
            "checksum": "a" * 64,
            "verified": True,
            "valid": False,
            "corrupt_chunks": [1],
            "errors": ["chunk 1 checksum mismatch"],
        }

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig(
            "http://testserver",
            retry_policy=no_delay_retry_policy,
        )
        async with StorageNodeClient(config, client=transport_client) as node:
            with pytest.raises(StorageNodeIntegrityError) as exc_info:
                await node.verify_object("obj", "ver")
            assert exc_info.value.detail["corrupt_chunks"] == [1]


@pytest.mark.asyncio
async def test_verify_requires_explicit_verified_true(no_delay_retry_policy: RetryPolicy) -> None:
    app = FastAPI()

    @app.get("/internal/v1/objects/{object_id}/{version_id}/verify")
    async def verify(object_id: str, version_id: str) -> dict[str, object]:
        del object_id, version_id
        return {
            "object_id": "obj",
            "version_id": "ver",
            "size_bytes": 1,
            "checksum": "a" * 64,
        }

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig("http://testserver", retry_policy=no_delay_retry_policy)
        async with StorageNodeClient(config, client=transport_client) as node:
            with pytest.raises(StorageNodeProtocolError):
                await node.verify_object("obj", "ver")


@pytest.mark.asyncio
async def test_unreachable_node_is_explicit(no_delay_retry_policy: RetryPolicy) -> None:
    async with StorageNodeClient(
        StorageNodeClientConfig(
            "http://127.0.0.1:1",
            retry_policy=no_delay_retry_policy,
        )
    ) as node:
        with pytest.raises(StorageNodeUnavailableError):
            await node.health()


@pytest.mark.asyncio
async def test_safe_reads_retry_transient_http_failures(no_delay_retry_policy: RetryPolicy) -> None:
    app = FastAPI()
    attempts = 0

    @app.get("/internal/v1/health")
    async def health() -> dict[str, str]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return Response(status_code=503)
        return {"status": "healthy", "node_id": "node-retry"}

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig("http://testserver", retry_policy=no_delay_retry_policy)
        async with StorageNodeClient(config, client=transport_client) as node:
            result = await node.health()
            assert result.node_id == "node-retry"
            assert attempts == 3


@pytest.mark.asyncio
async def test_put_is_not_automatically_retried(no_delay_retry_policy: RetryPolicy) -> None:
    app = FastAPI()
    calls = 0

    @app.put("/internal/v1/objects/{object_id}/{version_id}")
    async def put(object_id: str, version_id: str, request: Request) -> Response:
        nonlocal calls
        del object_id, version_id
        calls += 1
        del request
        return Response(status_code=503)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig("http://testserver", retry_policy=no_delay_retry_policy)
        async with StorageNodeClient(config, client=transport_client) as node:
            with pytest.raises(StorageNodeProtocolError):
                await node.put_object("obj", "ver", b"x")
            assert calls == 1


@pytest.mark.asyncio
async def test_stream_get_retries_only_before_success(no_delay_retry_policy: RetryPolicy) -> None:
    app = FastAPI()
    calls = 0

    @app.get("/internal/v1/objects/{object_id}/{version_id}")
    async def get(object_id: str, version_id: str) -> Response:
        del object_id, version_id
        nonlocal calls
        calls += 1
        if calls < 2:
            return Response(status_code=503)
        return Response(content=b"hello", media_type="application/octet-stream")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as transport_client:
        config = StorageNodeClientConfig("http://testserver", retry_policy=no_delay_retry_policy)
        async with StorageNodeClient(config, client=transport_client) as node:
            async with node.stream_object("obj", "ver") as response:
                assert await response.aread() == b"hello"
            assert calls == 2


def test_client_rejects_path_traversal_identifiers() -> None:
    with pytest.raises(ValueError):
        StorageNodeClient("http://test")._object_path("..", "version")
    with pytest.raises(ValueError):
        StorageNodeClient("http://test")._object_path("object", "version/evil")
    with pytest.raises(ValueError):
        StorageNodeClient("http://test")._object_path("object", "version\x00evil")


def test_client_config_validates_address_and_timeouts() -> None:
    with pytest.raises(ValueError):
        StorageNodeClientConfig(" ", 1)
    with pytest.raises(ValueError):
        StorageNodeClientConfig("http://test", 0)
    with pytest.raises(ValueError):
        StorageNodeTimeouts(connect_seconds=0)


def test_retry_policy_validates_bounds() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(base_delay_seconds=-1)
    with pytest.raises(ValueError):
        RetryPolicy(max_delay_seconds=0, base_delay_seconds=1)


def test_generated_request_id_is_nonempty() -> None:
    config = StorageNodeClientConfig("http://test")
    client = StorageNodeClient(config)
    request_id = client._request_id(None)
    assert request_id.startswith("req_")
    assert len(request_id) > 8

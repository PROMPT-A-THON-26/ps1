from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from hashlib import sha256

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from common.constants import NodeState, ObjectState, VersionState
from common.errors import VersionConflict
from gateway.api import build_gateway_router
from metadata.manager import MetadataManager
from replication.node_client import (
    StorageObjectAlreadyExistsError,
    StorageNodeUnavailableError,
    StoredObject,
    VerifiedObject,
)

SHA_A = "a" * 64
STORAGE: dict[tuple[str, str, str], bytes] = {}
FAIL_NODES: set[str] = set()


class FakeResponse:
    def __init__(self, body: bytes, *, fail: bool = False) -> None:
        self.body = body
        self.fail = fail

    async def aiter_bytes(self):
        if self.fail:
            raise StorageNodeUnavailableError("stream failed")
        yield self.body


class FakeClient:
    def __init__(self, address: str) -> None:
        self.node_id = address.split("//", 1)[-1].split(":", 1)[0]

    async def put_object(self, object_id: str, version_id: str, data, **kwargs):
        if self.node_id in FAIL_NODES:
            raise StorageNodeUnavailableError("unavailable")
        key = (self.node_id, object_id, version_id)
        if key in STORAGE:
            raise StorageObjectAlreadyExistsError("already exists")
        if hasattr(data, "__aiter__"):
            parts = []
            async for chunk in data:
                parts.append(bytes(chunk))
            body = b"".join(parts)
        else:
            body = bytes(data)
        STORAGE[key] = body
        return StoredObject(object_id, version_id, len(body))

    async def verify_object(self, object_id: str, version_id: str, **kwargs):
        if self.node_id in FAIL_NODES:
            raise StorageNodeUnavailableError("unavailable")
        body = STORAGE[(self.node_id, object_id, version_id)]
        return VerifiedObject(
            object_id,
            version_id,
            len(body),
            sha256(body).hexdigest(),
            True,
            True,
        )

    async def head_object(self, object_id: str, version_id: str, **kwargs):
        if self.node_id in FAIL_NODES:
            raise StorageNodeUnavailableError("unavailable")
        return len(STORAGE[(self.node_id, object_id, version_id)])

    @asynccontextmanager
    async def stream_object(self, object_id: str, version_id: str, **kwargs):
        if self.node_id in FAIL_NODES:
            raise StorageNodeUnavailableError("unavailable")
        yield FakeResponse(STORAGE[(self.node_id, object_id, version_id)])

    async def delete_object(self, object_id: str, version_id: str, **kwargs):
        if self.node_id in FAIL_NODES:
            raise StorageNodeUnavailableError("unavailable")
        STORAGE.pop((self.node_id, object_id, version_id), None)

    async def aclose(self) -> None:
        return None


def make_app(db_session) -> FastAPI:
    @contextmanager
    def session_factory():
        yield db_session

    app = FastAPI()
    app.include_router(
        build_gateway_router(
            session_factory,
            client_factory=FakeClient,
        )
    )
    return app


def seed_nodes(db_session, count: int = 3):
    manager = MetadataManager(db_session)
    for index in range(count):
        node_id = f"node-{chr(ord('a') + index)}"
        manager.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=10_000,
            status=NodeState.HEALTHY,
        )
    db_session.commit()


@pytest.fixture(autouse=True)
def reset_fake_storage():
    STORAGE.clear()
    FAIL_NODES.clear()


@pytest.mark.asyncio
async def test_gateway_put_creates_replicated_committed_version(db_session):
    seed_nodes(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        response = await client.put(
            "/api/v1/objects/hello.txt",
            content=b"hello",
            headers={"X-Request-ID": "req-test-1"},
        )

    assert response.status_code == 201
    body = response.json()
    assert body["name"] == "hello.txt"
    assert body["version_number"] == 1
    assert body["size_bytes"] == 5
    assert body["checksum"] == sha256(b"hello").hexdigest()
    assert len(body["replicas"]) == 3
    assert response.headers["x-request-id"] == "req-test-1"


@pytest.mark.asyncio
async def test_gateway_put_enforces_expected_version(db_session):
    seed_nodes(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.put(
            "/api/v1/objects/versions.txt",
            content=b"one",
        )
        conflict = await client.put(
            "/api/v1/objects/versions.txt",
            content=b"two",
            headers={"X-Expected-Version": "0"},
        )
        second = await client.put(
            "/api/v1/objects/versions.txt",
            content=b"two",
            headers={"X-Expected-Version": "1"},
        )

    assert first.status_code == 201
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "VERSION_CONFLICT"
    assert second.status_code == 201
    assert second.json()["version_number"] == 2


@pytest.mark.asyncio
async def test_gateway_get_streams_current_version_and_fails_over_before_body(db_session):
    seed_nodes(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        upload = await client.put("/api/v1/objects/download.bin", content=b"payload")
        object_id = upload.json()["object_id"]
        version_id = upload.json()["version_id"]
        FAIL_NODES.add("node-a")
        response = await client.get("/api/v1/objects/download.bin")

    assert response.status_code == 200
    assert response.content == b"payload"
    assert response.headers["content-length"] == "7"
    assert response.headers["x-version-id"] == version_id
    assert STORAGE[("node-b", object_id, version_id)] == b"payload"


@pytest.mark.asyncio
async def test_gateway_delete_removes_storage_copies_and_marks_object_deleted(db_session):
    seed_nodes(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        upload = await client.put("/api/v1/objects/remove.bin", content=b"payload")
        object_id = upload.json()["object_id"]
        version_id = upload.json()["version_id"]
        deleted = await client.delete("/api/v1/objects/remove.bin")
        missing = await client.get("/api/v1/objects/remove.bin/metadata")

    assert deleted.status_code == 204
    assert missing.status_code == 404
    assert all(
        key not in STORAGE
        for key in (
            ("node-a", object_id, version_id),
            ("node-b", object_id, version_id),
            ("node-c", object_id, version_id),
        )
    )

    manager = MetadataManager(db_session)
    obj = manager.get_object("remove.bin")
    assert obj is not None
    assert obj.state is ObjectState.DELETED


@pytest.mark.asyncio
async def test_gateway_delete_leaves_deleting_state_when_a_node_is_unavailable(db_session):
    seed_nodes(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        await client.put("/api/v1/objects/retry-delete.bin", content=b"payload")
        FAIL_NODES.add("node-b")
        response = await client.delete("/api/v1/objects/retry-delete.bin")

    assert response.status_code == 503
    manager = MetadataManager(db_session)
    obj = manager.get_object("retry-delete.bin")
    assert obj is not None
    assert obj.state is ObjectState.DELETING


@pytest.mark.asyncio
async def test_gateway_head_exposes_committed_version_metadata(db_session):
    seed_nodes(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        await client.put("/api/v1/objects/head.txt", content=b"hello")
        response = await client.head("/api/v1/objects/head.txt")

    assert response.status_code == 200
    assert response.headers["content-length"] == "5"
    assert response.headers["x-version-number"] == "1"
    assert response.headers["x-checksum-sha256"] == sha256(b"hello").hexdigest()


def test_gateway_health_reports_degraded_until_all_registered_nodes_are_healthy(db_session):
    manager = MetadataManager(db_session)
    manager.register_node(
        node_id="joining-node",
        address="http://joining-node:9001",
        capacity_bytes=10_000,
        status=NodeState.JOINING,
    )
    manager.register_node(
        node_id="healthy-node",
        address="http://healthy-node:9001",
        capacity_bytes=10_000,
        status=NodeState.HEALTHY,
    )
    health = __import__("gateway.service", fromlist=["GatewayService"]).GatewayService(db_session).health()
    assert health["status"] == "degraded"
    assert health["nodes"] == 2

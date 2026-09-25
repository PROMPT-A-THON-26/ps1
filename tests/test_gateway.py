from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from common.constants import NodeState
from gateway.api import build_gateway_router
from metadata.manager import MetadataManager


SHA_A = "a" * 64


def make_app(db_session):
    @contextmanager
    def session_factory():
        yield db_session
    app = FastAPI()
    app.include_router(build_gateway_router(session_factory))
    return app


def seed(db_session):
    manager = MetadataManager(db_session)
    obj = manager.create_object("hello.txt")
    version = manager.create_version(obj.object_id, size_bytes=5, checksum=SHA_A)
    manager.commit_version(version.version_id)
    manager.register_node(
        node_id="node-gw",
        address="http://node-gw:9001",
        capacity_bytes=1000,
        status=NodeState.HEALTHY,
    )
    db_session.commit()
    return obj, version


@pytest.mark.asyncio
async def test_gateway_exposes_object_metadata_and_versions(db_session):
    seed(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        metadata = await client.get("/api/v1/objects/hello.txt/metadata")
        versions = await client.get("/api/v1/objects/hello.txt/versions")
        listing = await client.get("/api/v1/objects")

    assert metadata.status_code == 200
    assert metadata.json()["name"] == "hello.txt"
    assert metadata.json()["current_version"] == 1
    assert versions.status_code == 200
    assert versions.json()[0]["checksum"] == SHA_A
    assert versions.json()[0]["healthy_replicas"] == 0
    assert listing.status_code == 200
    assert listing.json()[0]["name"] == "hello.txt"


@pytest.mark.asyncio
async def test_gateway_head_returns_version_headers(db_session):
    seed(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.head("/api/v1/objects/hello.txt")

    assert response.status_code == 200
    assert response.headers["content-length"] == "5"
    assert response.headers["x-version-number"] == "1"
    assert response.headers["x-checksum-sha256"] == SHA_A


@pytest.mark.asyncio
async def test_gateway_lists_nodes_and_health(db_session):
    seed(db_session)
    app = make_app(db_session)

    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        nodes = await client.get("/api/v1/nodes")
        health = await client.get("/api/v1/health")

    assert nodes.status_code == 200
    assert nodes.json()[0]["node_id"] == "node-gw"
    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert health.json()["node_states"]["HEALTHY"] == 1


@pytest.mark.asyncio
async def test_gateway_returns_404_for_unknown_object(db_session):
    app = make_app(db_session)
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.get("/api/v1/objects/missing/metadata")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_gateway_reserves_write_contract_until_replication(db_session):
    app = make_app(db_session)
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.put("/api/v1/objects/new.txt", content=b"hello")
    assert response.status_code == 501


@pytest.mark.asyncio
async def test_gateway_reserves_delete_contract_until_replication(db_session):
    app = make_app(db_session)
    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.delete("/api/v1/objects/new.txt")
    assert response.status_code == 501

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from health.api import build_heartbeat_router
from health.heartbeat import HeartbeatPayload
from health.node_registry import NodeRegistry


@pytest.mark.asyncio
async def test_heartbeat_router_accepts_registered_node(db_session):
    NodeRegistry(db_session).register(
        node_id="api-node",
        address="http://api-node:8001",
        capacity_bytes=1000,
    )

    @contextmanager
    def session_factory():
        yield db_session

    app = FastAPI()
    app.include_router(build_heartbeat_router(session_factory))

    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post(
            "/internal/v1/heartbeat",
            json={
                "node_id": "api-node",
                "capacity_bytes": 1000,
                "used_bytes": 100,
                "timestamp": datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc).isoformat(),
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] is True
    assert body["status"] == "HEALTHY"
    assert body["free_bytes"] == 900


@pytest.mark.asyncio
async def test_heartbeat_router_returns_404_for_unknown_node(db_session):
    @contextmanager
    def session_factory():
        yield db_session

    app = FastAPI()
    app.include_router(build_heartbeat_router(session_factory))

    async with httpx.AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        response = await client.post(
            "/internal/v1/heartbeat",
            json={
                "node_id": "missing-node",
                "capacity_bytes": 1000,
                "used_bytes": 100,
                "timestamp": datetime(2026, 9, 25, 20, 0, tzinfo=timezone.utc).isoformat(),
            },
        )

    assert response.status_code == 404

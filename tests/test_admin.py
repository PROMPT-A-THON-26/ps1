from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from common.constants import NodeState, ReplicaState
from gateway.api import build_gateway_router
from metadata.manager import MetadataManager


DATA = b"admin-payload"
CHECKSUM = sha256(DATA).hexdigest()


def _app(db_session):
    @contextmanager
    def session_factory():
        yield db_session

    app = FastAPI()
    app.include_router(build_gateway_router(session_factory))
    return app


def _seed_version(db_session):
    metadata = MetadataManager(db_session)
    obj = metadata.create_object("admin.txt")
    version = metadata.create_version(
        obj.object_id,
        size_bytes=len(DATA),
        checksum=CHECKSUM,
    )
    for node_id, capacity in (
        ("node-a", 1000),
        ("node-b", 900),
        ("node-c", 800),
        ("node-d", 700),
    ):
        metadata.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=capacity,
            status=NodeState.HEALTHY,
        )
    replica = metadata.create_replica(version.version_id, "node-a")
    metadata.set_replica_state(replica.replica_id, ReplicaState.COPYING)
    metadata.mark_replica_healthy(
        replica.replica_id,
        checksum=CHECKSUM,
        size_bytes=len(DATA),
    )
    metadata.commit_version(version.version_id)
    db_session.commit()
    return version, replica


@pytest.mark.asyncio
async def test_admin_repair_creates_durable_job_and_dispatches_worker(db_session, monkeypatch):
    version, _ = _seed_version(db_session)
    calls = []

    def fake_delay(repair_id):
        calls.append(repair_id)
        return SimpleNamespace(id="task-repair-1")

    monkeypatch.setattr("gateway.admin.run_repair_job.delay", fake_delay)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=_app(db_session)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/admin/repair",
            json={"version_id": str(version.version_id)},
        )
        repair_id = response.json()["job_id"]
        status_response = await client.get(
            f"/api/v1/admin/repair/{repair_id}"
        )

    assert response.status_code == 202
    assert calls == [repair_id]
    assert status_response.status_code == 200
    assert status_response.json()["status"] == "PENDING"


@pytest.mark.asyncio
async def test_admin_integrity_and_rebalance_dispatch_background_tasks(db_session, monkeypatch):
    version, replica = _seed_version(db_session)
    calls = []

    monkeypatch.setattr(
        "gateway.admin.verify_replica.delay",
        lambda replica_id: calls.append(("verify", replica_id)) or SimpleNamespace(id="task-integrity-1"),
    )
    monkeypatch.setattr(
        "gateway.admin.rebalance_node.delay",
        lambda node_id: calls.append(("rebalance", node_id)) or SimpleNamespace(id="task-rebalance-1"),
    )
    monkeypatch.setattr("gateway.admin._celery_status", lambda task_id: "PENDING")

    async with httpx.AsyncClient(
        transport=ASGITransport(app=_app(db_session)),
        base_url="http://testserver",
    ) as client:
        integrity = await client.post(
            "/api/v1/admin/integrity/check",
            json={"replica_id": str(replica.replica_id)},
        )
        rebalance = await client.post(
            "/api/v1/admin/rebalance",
            json={"node_id": "node-a"},
        )

    assert integrity.status_code == 202
    assert integrity.json()["job_id"] == "task-integrity-1"
    assert rebalance.status_code == 202
    assert rebalance.json()["job_id"] == "task-rebalance-1"
    assert calls == [
        ("verify", str(replica.replica_id)),
        ("rebalance", "node-a"),
    ]


@pytest.mark.asyncio
async def test_admin_integrity_requires_exactly_one_target(db_session):
    _seed_version(db_session)
    async with httpx.AsyncClient(
        transport=ASGITransport(app=_app(db_session)),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/admin/integrity/check",
            json={},
        )
    assert response.status_code == 422

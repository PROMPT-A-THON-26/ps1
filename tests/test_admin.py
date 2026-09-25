from contextlib import contextmanager
from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from httpx import ASGITransport

from common.constants import NodeState, ReplicaState
from gateway.api import build_gateway_router
from metadata.manager import MetadataManager
from metadata.models import IntegrityJob, RebalanceJob, RepairJob
from worker import tasks


def _app(session, monkeypatch):
    @contextmanager
    def factory():
        yield session

    def queued(*, args=None, kwargs=None):
        return SimpleNamespace(id="test-task")

    for name in ("repair_version", "run_integrity_check", "migrate_replica"):
        monkeypatch.setattr(getattr(tasks, name), "apply_async", queued)

    app = FastAPI()
    app.include_router(build_gateway_router(factory))
    return app


def _seed(session):
    metadata = MetadataManager(session)
    obj = metadata.create_object("admin.bin")
    version = metadata.create_version(
        obj.object_id, size_bytes=4, checksum=sha256(b"data").hexdigest()
    )
    for node_id in ("node-a", "node-b", "node-c"):
        metadata.register_node(
            node_id=node_id,
            address=f"http://{node_id}:9001",
            capacity_bytes=100,
            status=NodeState.HEALTHY,
        )
    for node_id in ("node-a",):
        replica = metadata.create_replica(version.version_id, node_id)
        metadata.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        metadata.mark_replica_healthy(
            replica.replica_id,
            checksum=version.checksum,
            size_bytes=version.size_bytes,
        )
    metadata.commit_version(version.version_id)
    session.commit()
    return version


@pytest.mark.asyncio
async def test_admin_job_endpoints_create_and_read(db_session, monkeypatch):
    version = _seed(db_session)
    app = _app(db_session, monkeypatch)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        repair = await client.post(
            "/api/v1/admin/repair",
            json={"version_id": str(version.version_id)},
        )
        integrity = await client.post(
            "/api/v1/admin/integrity/check",
            json={"version_id": str(version.version_id)},
        )

    assert repair.status_code == 202
    assert integrity.status_code == 202
    assert repair.json()["queued"] is True
    assert integrity.json()["queued"] is True

    repair_id = repair.json()["repair_id"]
    integrity_id = integrity.json()["integrity_id"]
    assert db_session.get(RepairJob, UUID(repair_id)) is not None
    assert db_session.get(IntegrityJob, UUID(integrity_id)) is not None

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        repair_status = await client.get(f"/api/v1/admin/repair/{repair_id}")
        integrity_status = await client.get(
            f"/api/v1/admin/integrity/check/{integrity_id}"
        )

    assert repair_status.status_code == 200
    assert integrity_status.status_code == 200
    assert repair_status.json()["status"] == "PENDING"
    assert integrity_status.json()["status"] == "PENDING"

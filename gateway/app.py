"""Production FastAPI application for the Vault public gateway."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from common.constants import NodeState
from common.settings import settings
from gateway.api import build_gateway_router
from metadata.database import create_schema, session_scope
from metadata.manager import MetadataManager
from replication import ReplicationPolicy


def _parse_storage_nodes() -> list[tuple[str, str]]:
    raw = os.getenv(
        "VAULT_STORAGE_NODES",
        ",".join(
            [
                "node-01=http://localhost:9001",
                "node-02=http://localhost:9002",
                "node-03=http://localhost:9003",
                "node-04=http://localhost:9004",
            ]
        ),
    )
    nodes: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        node_id, address = item.split("=", 1)
        node_id, address = node_id.strip(), address.strip()
        if node_id and address:
            nodes.append((node_id, address.rstrip("/")))
    return nodes


def _bootstrap_nodes() -> None:
    create_schema()
    capacity = int(os.getenv("VAULT_NODE_CAPACITY_BYTES", str(10 * 1024**3)))
    with session_scope() as session:
        manager = MetadataManager(session)
        for node_id, address in _parse_storage_nodes():
            manager.register_node(
                node_id=node_id,
                address=address,
                capacity_bytes=capacity,
                status=NodeState.HEALTHY,
            )
        session.commit()


@asynccontextmanager
async def lifespan(_: FastAPI):
    _bootstrap_nodes()
    yield


app = FastAPI(
    title="Vault Gateway",
    version="1.0.0",
    description="Public control-plane API for the Vault distributed object store.",
    lifespan=lifespan,
)

cors_raw = os.getenv(
    "CORS_ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173",
)
cors_origins = [item.strip() for item in cors_raw.split(",") if item.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    build_gateway_router(
        session_scope,
        replication_policy=ReplicationPolicy.from_settings(),
    )
)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "service": "vault-gateway"}


@app.get("/")
def root() -> dict[str, str]:
    return {
        "service": "vault-gateway",
        "api": "/api/v1",
        "status": "ok",
    }


__all__ = ["app"]

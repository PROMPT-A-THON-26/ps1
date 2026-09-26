"""Production FastAPI application for the Vault public gateway."""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from sqlalchemy import select

from common.constants import NodeState
from common.settings import settings
from gateway.api import build_gateway_router
from replication.node_client import StorageNodeClient, StorageNodeClientError
from metadata.database import create_schema, session_scope
from metadata.manager import MetadataManager
from metadata.models import StorageNode
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


async def _verify_storage_node(
    node_id: str,
    address: str,
) -> tuple[str, str, int] | None:
    """Verify a fresh node before allowing it to enter HEALTHY state."""
    client = StorageNodeClient(
        address,
        timeout_seconds=settings.storage_request_timeout_seconds,
    )
    try:
        health, stats = await asyncio.gather(client.health(), client.stats())
        if health.node_id != node_id or health.status.lower() != "healthy":
            return None
        return node_id, address, stats.capacity_bytes
    except StorageNodeClientError:
        return None
    finally:
        await client.aclose()


async def _bootstrap_nodes() -> None:
    create_schema()
    configured_capacity = int(
        os.getenv("VAULT_NODE_CAPACITY_BYTES", str(10 * 1024**3))
    )
    configured_nodes = _parse_storage_nodes()
    verified = await asyncio.gather(
        *(_verify_storage_node(node_id, address) for node_id, address in configured_nodes)
    )
    verified_by_id = {
        node_id: (address, capacity)
        for result in verified
        if result is not None
        for node_id, address, capacity in [result]
    }

    with session_scope() as session:
        manager = MetadataManager(session)
        for node_id, address in configured_nodes:
            existing = session.scalar(
                select(StorageNode).where(StorageNode.address == address)
            )
            if existing is not None:
                continue
            verified_result = verified_by_id.get(node_id)
            manager.register_node(
                node_id=node_id,
                address=address,
                capacity_bytes=(
                    verified_result[1] if verified_result is not None else configured_capacity
                ),
                # A new node is HEALTHY only after the storage-node API has
                # been contacted and its identity/status/capacity verified.
                status=(
                    NodeState.HEALTHY
                    if verified_result is not None
                    else NodeState.JOINING
                ),
            )
        session.commit()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await _bootstrap_nodes()
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
    allow_methods=["GET", "HEAD", "PUT", "DELETE", "POST", "OPTIONS"],
    allow_headers=["Accept", "Content-Type", "X-Expected-Version", "X-Request-ID"],
)

app.include_router(
    build_gateway_router(
        session_scope,
        replication_policy=ReplicationPolicy.from_settings(),
    )
)


@app.middleware("http")
async def security_headers(request: Request, call_next) -> Response:
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Cache-Control", "no-store")
    response.headers.setdefault("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'; base-uri 'none'")
    response.headers.setdefault("Permissions-Policy", "accelerometer=(), camera=(), geolocation=(), microphone=(), payment=(), usb=()")
    response.headers.setdefault("X-Permitted-Cross-Domain-Policies", "none")
    return response


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

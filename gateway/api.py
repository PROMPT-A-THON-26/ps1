"""FastAPI routes for the public Vault gateway contract."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Callable

from fastapi import APIRouter, HTTPException, Response, status
from sqlalchemy.orm import Session

from common.errors import ObjectNotFound, VaultError
from .service import GatewayService


def build_gateway_router(
    session_factory: Callable[[], AbstractContextManager[Session]],
) -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get("/objects")
    def list_objects() -> list[dict]:
        with session_factory() as session:
            return [
                {
                    "object_id": str(obj.object_id),
                    "name": obj.name,
                    "state": obj.state.value,
                    "current_version_id": None if obj.current_version_id is None else str(obj.current_version_id),
                    "created_at": obj.created_at,
                    "updated_at": obj.updated_at,
                }
                for obj in GatewayService(session).list_objects()
            ]

    @router.get("/objects/{name}/metadata")
    def object_metadata(name: str) -> dict:
        try:
            with session_factory() as session:
                return GatewayService(session).object_metadata(name)
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc

    @router.get("/objects/{name}/versions")
    def object_versions(name: str) -> list[dict]:
        try:
            with session_factory() as session:
                return GatewayService(session).versions(name)
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc

    @router.get("/objects/{name}")
    def get_object(name: str) -> dict:
        """Return logical-object metadata; byte streaming is integrated with replication in the next step."""
        try:
            with session_factory() as session:
                obj, version = GatewayService(session).head(name)
                return {
                    "object_id": str(obj.object_id),
                    "name": obj.name,
                    "state": obj.state.value,
                    "version_id": None if version is None else str(version.version_id),
                    "version_number": None if version is None else version.version_number,
                    "size_bytes": None if version is None else version.size_bytes,
                    "checksum": None if version is None else version.checksum,
                }
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc

    @router.head("/objects/{name}")
    def head_object(name: str) -> Response:
        try:
            with session_factory() as session:
                _, version = GatewayService(session).head(name)
                if version is None:
                    return Response(status_code=status.HTTP_404_NOT_FOUND)
                return Response(
                    status_code=status.HTTP_200_OK,
                    headers={
                        "Content-Length": str(version.size_bytes),
                        "X-Version-ID": str(version.version_id),
                        "X-Version-Number": str(version.version_number),
                        "X-Checksum-SHA256": version.checksum,
                    },
                )
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc

    @router.get("/nodes")
    def list_nodes() -> list[dict]:
        with session_factory() as session:
            return [
                {
                    "node_id": node.node_id,
                    "address": node.address,
                    "status": node.status.value,
                    "capacity_bytes": node.capacity_bytes,
                    "used_bytes": node.used_bytes,
                    "free_bytes": node.free_bytes,
                    "last_heartbeat_at": node.last_heartbeat_at,
                }
                for node in GatewayService(session).list_nodes()
            ]

    @router.get("/nodes/{node_id}")
    def get_node(node_id: str) -> dict:
        try:
            with session_factory() as session:
                node = GatewayService(session).get_node(node_id)
                return {
                    "node_id": node.node_id,
                    "address": node.address,
                    "status": node.status.value,
                    "capacity_bytes": node.capacity_bytes,
                    "used_bytes": node.used_bytes,
                    "free_bytes": node.free_bytes,
                    "last_heartbeat_at": node.last_heartbeat_at,
                }
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc

    @router.get("/health")
    def health() -> dict:
        with session_factory() as session:
            return GatewayService(session).health()

    @router.put("/objects/{name}")
    def put_object(name: str) -> Response:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="object upload is wired in the replication milestone; the gateway contract is reserved here",
        )

    @router.delete("/objects/{name}")
    def delete_object(name: str) -> Response:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="object deletion is wired in the replication milestone; the gateway contract is reserved here",
        )

    return router

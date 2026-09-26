"""FastAPI routes for the public Vault gateway contract."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Callable
from uuid import UUID

import httpx
from fastapi import APIRouter, Header, Request, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy.orm import Session

from common.constants import ErrorCode
from common.errors import VaultError
from common.ids import normalize_request_id
from replication.node_client import StorageNodeClient, StorageNodeClientError

from .admin_service import AdminService, IntegrityRequest, RebalanceRequest, RepairRequest

from .service import GatewayService


def _request_id(request: Request) -> str:
    """Return a bounded, log-safe request ID for the full request lifecycle."""
    return normalize_request_id(request.headers.get("X-Request-ID"))



def _invalid_request_response(exc: ValueError, request_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": {
                "code": ErrorCode.INVALID_REQUEST.value,
                "message": str(exc),
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )


def _error_response(exc: VaultError, request_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code.value,
                "message": exc.message,
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )


def build_gateway_router(
    session_factory: Callable[[], AbstractContextManager[Session]],
    *,
    replication_policy=None,
    client_factory=StorageNodeClient,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get("/objects")
    def list_objects(request: Request) -> JSONResponse:
        request_id = _request_id(request)
        with session_factory() as session:
            payload = [
                {
                    "object_id": str(obj.object_id),
                    "name": obj.name,
                    "state": obj.state.value,
                    "current_version_id": (
                        None
                        if obj.current_version_id is None
                        else str(obj.current_version_id)
                    ),
                    "created_at": obj.created_at,
                    "updated_at": obj.updated_at,
                }
                for obj in GatewayService(session).list_objects()
            ]
        return JSONResponse(jsonable_encoder(payload), headers={"X-Request-ID": request_id})

    @router.get("/objects/{name}/metadata")
    def object_metadata(name: str, request: Request) -> JSONResponse:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                payload = GatewayService(session).object_metadata(name)
            return JSONResponse(jsonable_encoder(payload), headers={"X-Request-ID": request_id})
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return _invalid_request_response(exc, request_id)

    @router.get("/objects/{name}/versions")
    def object_versions(name: str, request: Request) -> JSONResponse:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                payload = GatewayService(session).versions(name)
            return JSONResponse(jsonable_encoder(payload), headers={"X-Request-ID": request_id})
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return _invalid_request_response(exc, request_id)

    @router.get("/objects/{name}")
    async def get_object(name: str, request: Request) -> Response:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                service = GatewayService(session)
                obj, version, targets = service.read_targets(name)
                target_descriptors = [
                    (node.node_id, node.address) for node in targets
                ]
                selected, remaining = await service.preflight_read_target(
                    targets,
                    object_id=str(obj.object_id),
                    version_id=str(version.version_id),
                    client_factory=client_factory,
                )
                target_descriptors = [
                    (node.node_id, node.address) for node in [selected, *remaining]
                ]

            async def body():
                for index, (node_id, address) in enumerate(target_descriptors):
                    client = client_factory(address)
                    emitted = False
                    try:
                        async with client.stream_object(
                            str(obj.object_id),
                            str(version.version_id),
                        ) as storage_response:
                            async for chunk in storage_response.aiter_bytes():
                                emitted = True
                                yield chunk
                        return
                    except (StorageNodeClientError, httpx.HTTPError):
                        if emitted or index == len(target_descriptors) - 1:
                            raise
                        continue
                    finally:
                        await client.aclose()

            return StreamingResponse(
                body(),
                status_code=status.HTTP_200_OK,
                media_type="application/octet-stream",
                headers={
                    "Content-Length": str(version.size_bytes),
                    "X-Version-ID": str(version.version_id),
                    "X-Version-Number": str(version.version_number),
                    "X-Checksum-SHA256": version.checksum,
                    "X-Request-ID": request_id,
                },
            )
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return _invalid_request_response(exc, request_id)
        except (StorageNodeClientError, httpx.HTTPError) as exc:
            return JSONResponse(
                status_code=503,
                content={
                    "error": {
                        "code": "NODE_UNAVAILABLE",
                        "message": str(exc),
                        "request_id": request_id,
                    }
                },
                headers={"X-Request-ID": request_id},
            )

    @router.head("/objects/{name}")
    def head_object(name: str, request: Request) -> Response:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                _, version = GatewayService(session).head(name)
                if version is None:
                    return Response(
                        status_code=status.HTTP_404_NOT_FOUND,
                        headers={"X-Request-ID": request_id},
                    )
                return Response(
                    status_code=status.HTTP_200_OK,
                    headers={
                        "Content-Length": str(version.size_bytes),
                        "X-Version-ID": str(version.version_id),
                        "X-Version-Number": str(version.version_number),
                        "X-Checksum-SHA256": version.checksum,
                        "X-Request-ID": request_id,
                    },
                )
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return _invalid_request_response(exc, request_id)

    @router.get("/nodes")
    def list_nodes(request: Request) -> JSONResponse:
        request_id = _request_id(request)
        with session_factory() as session:
            payload = [
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
        return JSONResponse(jsonable_encoder(payload), headers={"X-Request-ID": request_id})

    @router.get("/nodes/{node_id}")
    def get_node(node_id: str, request: Request) -> JSONResponse:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                node = GatewayService(session).get_node(node_id)
                payload = {
                    "node_id": node.node_id,
                    "address": node.address,
                    "status": node.status.value,
                    "capacity_bytes": node.capacity_bytes,
                    "used_bytes": node.used_bytes,
                    "free_bytes": node.free_bytes,
                    "last_heartbeat_at": node.last_heartbeat_at,
                }
            return JSONResponse(jsonable_encoder(payload), headers={"X-Request-ID": request_id})
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return _invalid_request_response(exc, request_id)

    @router.get("/health")
    def health(request: Request) -> JSONResponse:
        request_id = _request_id(request)
        with session_factory() as session:
            return JSONResponse(
                GatewayService(session).health(),
                headers={"X-Request-ID": request_id},
            )

    @router.put("/objects/{name}", status_code=status.HTTP_201_CREATED)
    async def put_object(
        name: str,
        request: Request,
        x_expected_version: int | None = Header(
            default=None,
            alias="X-Expected-Version",
        ),
    ) -> JSONResponse:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                payload = await GatewayService(session).put_object(
                    name,
                    request.stream(),
                    expected_current_version=x_expected_version,
                    replication_policy=replication_policy,
                    client_factory=client_factory,
                )
            return JSONResponse(
                status_code=status.HTTP_201_CREATED,
                content=payload,
                headers={"X-Request-ID": request_id},
            )
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={
                    "error": {
                        "code": "INVALID_REQUEST",
                        "message": str(exc),
                        "request_id": request_id,
                    }
                },
                headers={"X-Request-ID": request_id},
            )

    @router.delete("/objects/{name}")
    async def delete_object(name: str, request: Request) -> Response:
        request_id = _request_id(request)
        try:
            with session_factory() as session:
                await GatewayService(session).delete_object(
                    name,
                    client_factory=client_factory,
                )
            return Response(
                status_code=status.HTTP_204_NO_CONTENT,
                headers={"X-Request-ID": request_id},
            )
        except VaultError as exc:
            return _error_response(exc, request_id)
        except ValueError as exc:
            return _invalid_request_response(exc, request_id)


    @router.post("/admin/repair", status_code=status.HTTP_202_ACCEPTED)
    def admin_create_repair(payload: RepairRequest, request: Request) -> JSONResponse:
        rid = _request_id(request)
        try:
            with session_factory() as session:
                result = AdminService(session).create_repair(payload)
            return JSONResponse(status_code=202, content=result, headers={"X-Request-ID": rid})
        except VaultError as exc:
            return _error_response(exc, rid)
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={"error": {"code": "INVALID_REQUEST", "message": str(exc), "request_id": rid}},
                headers={"X-Request-ID": rid},
            )

    @router.get("/admin/repair/{repair_id}")
    def admin_get_repair(repair_id: UUID, request: Request) -> JSONResponse:
        rid = _request_id(request)
        try:
            with session_factory() as session:
                result = AdminService(session).get_repair(repair_id)
            return JSONResponse(result, headers={"X-Request-ID": rid})
        except VaultError as exc:
            return _error_response(exc, rid)

    @router.post("/admin/integrity/check", status_code=status.HTTP_202_ACCEPTED)
    def admin_create_integrity(payload: IntegrityRequest, request: Request) -> JSONResponse:
        rid = _request_id(request)
        try:
            with session_factory() as session:
                result = AdminService(session).create_integrity(payload)
            return JSONResponse(status_code=202, content=result, headers={"X-Request-ID": rid})
        except VaultError as exc:
            return _error_response(exc, rid)

    @router.get("/admin/integrity/check/{job_id}")
    def admin_get_integrity(job_id: UUID, request: Request) -> JSONResponse:
        rid = _request_id(request)
        try:
            with session_factory() as session:
                result = AdminService(session).get_integrity(job_id)
            return JSONResponse(result, headers={"X-Request-ID": rid})
        except VaultError as exc:
            return _error_response(exc, rid)

    @router.post("/admin/rebalance", status_code=status.HTTP_202_ACCEPTED)
    def admin_create_rebalance(payload: RebalanceRequest, request: Request) -> JSONResponse:
        rid = _request_id(request)
        try:
            with session_factory() as session:
                result = AdminService(session).create_rebalance(payload)
            return JSONResponse(status_code=202, content=result, headers={"X-Request-ID": rid})
        except VaultError as exc:
            return _error_response(exc, rid)
        except ValueError as exc:
            return JSONResponse(
                status_code=422,
                content={"error": {"code": "INVALID_REQUEST", "message": str(exc), "request_id": rid}},
                headers={"X-Request-ID": rid},
            )

    @router.get("/admin/rebalance/{job_id}")
    def admin_get_rebalance(job_id: UUID, request: Request) -> JSONResponse:
        rid = _request_id(request)
        try:
            with session_factory() as session:
                result = AdminService(session).get_rebalance(job_id)
            return JSONResponse(result, headers={"X-Request-ID": rid})
        except VaultError as exc:
            return _error_response(exc, rid)

    return router

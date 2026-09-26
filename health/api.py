"""Internal heartbeat HTTP router for the Vault control plane."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Callable

from fastapi import APIRouter, Header, HTTPException, Request, status

from common.ids import normalize_request_id
from sqlalchemy.orm import Session

from common.errors import InvalidState, ObjectNotFound

from .heartbeat import HeartbeatPayload, HeartbeatResult, HeartbeatService


def build_heartbeat_router(
    session_factory: Callable[[], AbstractContextManager[Session]],
) -> APIRouter:
    """Build the heartbeat router around a caller-provided SQLAlchemy factory."""
    router = APIRouter()

    @router.post(
        "/internal/v1/heartbeat",
        response_model=HeartbeatResult,
        status_code=status.HTTP_200_OK,
    )
    async def heartbeat(payload: HeartbeatPayload, request: Request, x_internal_api_key: str | None = Header(default=None, alias="X-Internal-API-Key")) -> HeartbeatResult:
        request_id = normalize_request_id(request.headers.get("X-Request-ID"))
        if not x_internal_api_key:
            raise HTTPException(status_code=401, detail="Valid internal API credentials are required.")
        try:
            with session_factory() as session:
                return await HeartbeatService(session).ingest_and_recover(payload)
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc
        except InvalidState as exc:
            raise HTTPException(status_code=409, detail=exc.message) from exc

    return router

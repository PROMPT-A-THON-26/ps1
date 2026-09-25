"""Internal heartbeat HTTP router for the Vault control plane."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Callable

from fastapi import APIRouter, HTTPException, status
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
    async def heartbeat(payload: HeartbeatPayload) -> HeartbeatResult:
        try:
            with session_factory() as session:
                return await HeartbeatService(session).ingest_and_recover(payload)
        except ObjectNotFound as exc:
            raise HTTPException(status_code=404, detail=exc.message) from exc
        except InvalidState as exc:
            raise HTTPException(status_code=409, detail=exc.message) from exc

    return router

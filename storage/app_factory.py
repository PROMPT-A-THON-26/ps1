from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from common.ids import new_request_id

from .node_lifecycle import NodeLifecycle, NodeLifecycleState
from .storage_engine import (
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    StorageEngine,
    StorageError,
    StorageFullError,
)


class HealthResponse(BaseModel):
    status: str
    node_id: str
    accepting_writes: bool


class StatsResponse(BaseModel):
    node_id: str
    capacity_bytes: int
    used_bytes: int
    free_bytes: int


class LifecycleResponse(BaseModel):
    status: str
    node_id: str
    accepting_writes: bool


class VerifyResponse(BaseModel):
    object_id: str
    version_id: str
    valid: bool
    checksum: str | None
    size_bytes: int
    chunk_count: int
    corrupt_chunks: list[int]
    errors: list[str]


def create_storage_node_app(
    engine: StorageEngine,
    node_id: str,
    lifecycle: NodeLifecycle,
) -> FastAPI:
    """Create an isolated storage-node app using the canonical Part-A handlers."""

    app = FastAPI(title="Vault Storage Node", version="0.1.0")

    @app.middleware("http")
    async def add_request_id(request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", "").strip()
        if not request_id:
            request_id = new_request_id()
        elif len(request_id) > 128:
            request_id = request_id[:128]

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        return response

    @app.get("/internal/v1/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        state = lifecycle.state
        return HealthResponse(
            status=state.value,
            node_id=node_id,
            accepting_writes=lifecycle.accepting_writes,
        )

    @app.get("/internal/v1/stats", response_model=StatsResponse)
    def stats() -> StatsResponse:
        current = engine.stats()
        return StatsResponse(
            node_id=node_id,
            capacity_bytes=current.capacity_bytes,
            used_bytes=current.used_bytes,
            free_bytes=current.free_bytes,
        )

    @app.post("/internal/v1/lifecycle/drain", response_model=LifecycleResponse)
    def drain_node() -> LifecycleResponse:
        lifecycle.drain()
        return LifecycleResponse(
            status=NodeLifecycleState.DRAINING.value,
            node_id=node_id,
            accepting_writes=False,
        )

    @app.post("/internal/v1/lifecycle/resume", response_model=LifecycleResponse)
    def resume_node() -> LifecycleResponse:
        lifecycle.resume()
        return LifecycleResponse(
            status=NodeLifecycleState.HEALTHY.value,
            node_id=node_id,
            accepting_writes=True,
        )

    @app.head("/internal/v1/objects/{object_id}/{version_id}")
    def head_object(object_id: str, version_id: str) -> Response:
        try:
            size = engine.object_size(object_id, version_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except ObjectNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc
        return Response(headers={"Content-Length": str(size)})

    @app.get("/internal/v1/objects/{object_id}/{version_id}")
    def get_object(object_id: str, version_id: str) -> StreamingResponse:
        try:
            size = engine.object_size(object_id, version_id)
            chunks = engine.iter_chunks(object_id, version_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except ObjectNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc

        return StreamingResponse(
            chunks,
            media_type="application/octet-stream",
            headers={"Content-Length": str(size)},
        )

    @app.put("/internal/v1/objects/{object_id}/{version_id}", status_code=201)
    async def put_object(object_id: str, version_id: str, request: Request) -> JSONResponse:
        if not lifecycle.accepting_writes:
            raise HTTPException(503, "storage node is draining and not accepting writes")
        try:
            size = await engine.write_stream(object_id, version_id, request.stream())
        except ObjectAlreadyExistsError as exc:
            raise HTTPException(409, str(exc)) from exc
        except StorageFullError as exc:
            raise HTTPException(507, str(exc)) from exc
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc
        except StorageError as exc:
            raise HTTPException(500, str(exc)) from exc

        return JSONResponse(
            {"object_id": object_id, "version_id": version_id, "size_bytes": size},
            status_code=201,
        )

    @app.delete("/internal/v1/objects/{object_id}/{version_id}", status_code=204)
    def delete_object(object_id: str, version_id: str) -> Response:
        try:
            engine.delete(object_id, version_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except ObjectNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc
        return Response(status_code=204)

    @app.get(
        "/internal/v1/objects/{object_id}/{version_id}/verify",
        response_model=VerifyResponse,
    )
    def verify_object(object_id: str, version_id: str) -> VerifyResponse:
        try:
            result = engine.verify(object_id, version_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except ObjectNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc

        return VerifyResponse(
            object_id=result.object_id,
            version_id=result.version_id,
            valid=result.valid,
            checksum=result.checksum,
            size_bytes=result.size_bytes,
            chunk_count=result.chunk_count,
            corrupt_chunks=list(result.corrupt_chunks),
            errors=list(result.errors),
        )

    return app

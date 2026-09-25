from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from .config import StorageNodeConfig
from .storage_engine import (
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    StorageEngine,
    StorageError,
    StorageFullError,
)

config = StorageNodeConfig.from_env()
engine = StorageEngine(
    config.data_dir,
    config.capacity_bytes,
    config.chunk_size_bytes,
)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    config.data_dir.mkdir(parents=True, exist_ok=True)
    yield


app = FastAPI(title="Vault Storage Node", version="0.1.0", lifespan=lifespan)


class HealthResponse(BaseModel):
    status: str
    node_id: str


class StatsResponse(BaseModel):
    node_id: str
    capacity_bytes: int
    used_bytes: int
    free_bytes: int


class VerifyResponse(BaseModel):
    object_id: str
    version_id: str
    size_bytes: int
    checksum: str | None
    verified: bool
    valid: bool
    chunk_count: int
    corrupt_chunks: list[int]
    errors: list[str]


def _request_id_headers(request: Request) -> dict[str, str]:
    request_id = request.headers.get("X-Request-ID")
    return {"X-Request-ID": request_id} if request_id else {}


@app.get("/internal/v1/health", response_model=HealthResponse)
def health(request: Request) -> JSONResponse:
    body = HealthResponse(status="healthy", node_id=config.node_id)
    return JSONResponse(body.model_dump(), headers=_request_id_headers(request))


@app.get("/internal/v1/stats", response_model=StatsResponse)
def stats(request: Request) -> JSONResponse:
    s = engine.stats()
    body = StatsResponse(
        node_id=config.node_id,
        capacity_bytes=s.capacity_bytes,
        used_bytes=s.used_bytes,
        free_bytes=s.free_bytes,
    )
    return JSONResponse(body.model_dump(), headers=_request_id_headers(request))


@app.head("/internal/v1/objects/{object_id}/{version_id}")
def head_object(object_id: str, version_id: str, request: Request) -> Response:
    try:
        size = engine.object_size(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    return Response(
        headers={
            "Content-Length": str(size),
            **_request_id_headers(request),
        }
    )


@app.get("/internal/v1/objects/{object_id}/{version_id}")
def get_object(object_id: str, version_id: str, request: Request) -> StreamingResponse:
    try:
        size = engine.object_size(object_id, version_id)
        chunks = engine.iter_chunks(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc

    return StreamingResponse(
        chunks,
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(size),
            **_request_id_headers(request),
        },
    )


@app.put(
    "/internal/v1/objects/{object_id}/{version_id}",
    status_code=status.HTTP_201_CREATED,
)
async def put_object(object_id: str, version_id: str, request: Request) -> JSONResponse:
    try:
        size = await engine.write_stream(object_id, version_id, request.stream())
    except ObjectAlreadyExistsError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except StorageFullError as exc:
        raise HTTPException(
            status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
            detail=str(exc),
        ) from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except StorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return JSONResponse(
        content={
            "object_id": object_id,
            "version_id": version_id,
            "size_bytes": size,
        },
        status_code=status.HTTP_201_CREATED,
        headers=_request_id_headers(request),
    )


@app.delete(
    "/internal/v1/objects/{object_id}/{version_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_object(object_id: str, version_id: str, request: Request) -> Response:
    try:
        engine.delete(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    return Response(
        status_code=status.HTTP_204_NO_CONTENT,
        headers=_request_id_headers(request),
    )


@app.get(
    "/internal/v1/objects/{object_id}/{version_id}/verify",
    response_model=VerifyResponse,
)
def verify_object(object_id: str, version_id: str, request: Request) -> JSONResponse:
    try:
        result = engine.verify(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except StorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    body = VerifyResponse(
        object_id=result.object_id,
        version_id=result.version_id,
        size_bytes=result.size_bytes,
        checksum=result.checksum,
        verified=True,
        valid=result.valid,
        chunk_count=result.chunk_count,
        corrupt_chunks=list(result.corrupt_chunks),
        errors=list(result.errors),
    )
    return JSONResponse(
        content=body.model_dump(),
        headers=_request_id_headers(request),
    )

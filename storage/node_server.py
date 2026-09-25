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
    StorageFullError,
    StorageError,
)


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
    checksum: str
    verified: bool = True


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


@app.get("/internal/v1/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="healthy", node_id=config.node_id)


@app.get("/internal/v1/stats", response_model=StatsResponse)
def stats() -> StatsResponse:
    current = engine.stats()
    return StatsResponse(
        node_id=config.node_id,
        capacity_bytes=current.capacity_bytes,
        used_bytes=current.used_bytes,
        free_bytes=current.free_bytes,
    )


@app.head("/internal/v1/objects/{object_id}/{version_id}")
def head_object(object_id: str, version_id: str) -> Response:
    try:
        size = engine.object_size(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return Response(headers={"Content-Length": str(size)})


@app.get("/internal/v1/objects/{object_id}/{version_id}")
def get_object(object_id: str, version_id: str) -> StreamingResponse:
    try:
        size = engine.object_size(object_id, version_id)
        chunks = engine.iter_chunks(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    return StreamingResponse(
        chunks,
        media_type="application/octet-stream",
        headers={"Content-Length": str(size)},
    )


@app.get(
    "/internal/v1/objects/{object_id}/{version_id}/verify",
    response_model=VerifyResponse,
)
def verify_object(object_id: str, version_id: str) -> VerifyResponse:
    try:
        size, checksum = engine.verify(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except StorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc

    return VerifyResponse(
        object_id=object_id,
        version_id=version_id,
        size_bytes=size,
        checksum=checksum,
        verified=True,
    )


@app.put(
    "/internal/v1/objects/{object_id}/{version_id}",
    status_code=status.HTTP_201_CREATED,
)
async def put_object(object_id: str, version_id: str, request: Request) -> JSONResponse:
    try:
        size = await engine.write_stream(object_id, version_id, request.stream())
    except ObjectAlreadyExistsError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except StorageFullError as exc:
        raise HTTPException(
            status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
            detail=str(exc),
        ) from exc
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
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
    )


@app.delete("/internal/v1/objects/{object_id}/{version_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_object(object_id: str, version_id: str) -> Response:
    try:
        engine.delete(object_id, version_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)

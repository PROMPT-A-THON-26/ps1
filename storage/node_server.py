from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .config import StorageNodeConfig
from .storage_engine import ObjectAlreadyExistsError, ObjectNotFoundError, StorageEngine, StorageFullError


class HealthResponse(BaseModel):
    status: str
    node_id: str


class StatsResponse(BaseModel):
    node_id: str
    capacity_bytes: int
    used_bytes: int
    free_bytes: int


config = StorageNodeConfig.from_env()
engine = StorageEngine(config.data_dir, config.capacity_bytes)


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
    return StatsResponse(node_id=config.node_id, capacity_bytes=current.capacity_bytes, used_bytes=current.used_bytes, free_bytes=current.free_bytes)


@app.head("/internal/v1/objects/{object_id}/{version_id}")
def head_object(object_id: str, version_id: str) -> Response:
    if not engine.exists(object_id, version_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Object version not found")
    path = engine.object_path(object_id, version_id)
    return Response(headers={"Content-Length": str(path.stat().st_size)})


@app.get("/internal/v1/objects/{object_id}/{version_id}")
def get_object(object_id: str, version_id: str) -> Response:
    try:
        data = engine.read_bytes(object_id, version_id)
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return Response(content=data, media_type="application/octet-stream")


@app.put("/internal/v1/objects/{object_id}/{version_id}", status_code=status.HTTP_201_CREATED)
def put_object(object_id: str, version_id: str, data: bytes) -> JSONResponse:
    try:
        size = engine.write_bytes(object_id, version_id, data)
    except ObjectAlreadyExistsError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except StorageFullError as exc:
        raise HTTPException(status_code=status.HTTP_507_INSUFFICIENT_STORAGE, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return JSONResponse(content={"object_id": object_id, "version_id": version_id, "size_bytes": size}, status_code=status.HTTP_201_CREATED)


@app.delete("/internal/v1/objects/{object_id}/{version_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_object(object_id: str, version_id: str) -> Response:
    try:
        engine.delete(object_id, version_id)
    except ObjectNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return Response(status_code=status.HTTP_204_NO_CONTENT)

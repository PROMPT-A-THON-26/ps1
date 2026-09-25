from __future__ import annotations

from collections.abc import AsyncIterator

import httpx


class ReplicaOperationError(Exception):
    """Raised when a node-to-node replica operation fails."""


async def stream_replica(
    source_url: str,
    object_id: str,
    version_id: str,
    *,
    timeout: float = 60.0,
) -> AsyncIterator[bytes]:
    """Stream one object from a trusted source node without buffering it."""
    url = (
        f"{source_url.rstrip('/')}/internal/v1/objects/"
        f"{object_id}/{version_id}"
    )
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            async with client.stream("GET", url) as response:
                if response.status_code == 404:
                    raise ReplicaOperationError("source object does not exist")
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    if chunk:
                        yield chunk
        except httpx.HTTPError as exc:
            raise ReplicaOperationError(
                f"source node request failed: {exc}"
            ) from exc


async def copy_replica(
    source_url: str,
    destination_url: str,
    object_id: str,
    version_id: str,
    *,
    timeout: float = 60.0,
) -> int:
    """Stream an object from one node directly into another node."""
    destination = (
        f"{destination_url.rstrip('/')}/internal/v1/objects/"
        f"{object_id}/{version_id}"
    )

    async def source_stream() -> AsyncIterator[bytes]:
        async for chunk in stream_replica(
            source_url,
            object_id,
            version_id,
            timeout=timeout,
        ):
            yield chunk

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.put(
                destination,
                content=source_stream(),
            )
        except httpx.HTTPError as exc:
            raise ReplicaOperationError(
                f"destination node request failed: {exc}"
            ) from exc

    if response.status_code == 409:
        raise ReplicaOperationError("destination replica already exists")
    if response.status_code == 507:
        raise ReplicaOperationError("destination node is out of storage")
    if response.status_code >= 400:
        raise ReplicaOperationError(
            f"destination node rejected replica: HTTP {response.status_code}"
        )

    try:
        body = response.json()
        return int(body["size_bytes"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ReplicaOperationError(
            "destination node returned an invalid replica response"
        ) from exc

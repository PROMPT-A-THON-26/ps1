from __future__ import annotations

from collections.abc import AsyncIterator

import httpx


class ReplicaOperationError(Exception):
    """Raised when a node-to-node replica operation fails."""


async def _verify_remote(
    client: httpx.AsyncClient,
    base_url: str,
    object_id: str,
    version_id: str,
) -> None:
    url = (
        f"{base_url.rstrip('/')}/internal/v1/objects/"
        f"{object_id}/{version_id}/verify"
    )
    try:
        response = await client.get(url)
        if response.status_code == 404:
            raise ReplicaOperationError("remote object does not exist")
        response.raise_for_status()
        body = response.json()
    except httpx.HTTPError as exc:
        raise ReplicaOperationError(f"verification request failed: {exc}") from exc
    except ValueError as exc:
        raise ReplicaOperationError("verification endpoint returned invalid JSON") from exc

    if body.get("valid") is not True:
        errors = body.get("errors") or ["remote object failed integrity verification"]
        raise ReplicaOperationError("; ".join(str(error) for error in errors))


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
            raise ReplicaOperationError(f"source node request failed: {exc}") from exc


async def copy_replica(
    source_url: str,
    destination_url: str,
    object_id: str,
    version_id: str,
    *,
    timeout: float = 60.0,
) -> int:
    """Copy a verified object between storage nodes without buffering it."""
    destination = (
        f"{destination_url.rstrip('/')}/internal/v1/objects/"
        f"{object_id}/{version_id}"
    )

    async def source_stream() -> AsyncIterator[bytes]:
        async for chunk in stream_replica(source_url, object_id, version_id, timeout=timeout):
            yield chunk

    async with httpx.AsyncClient(timeout=timeout) as client:
        await _verify_remote(client, source_url, object_id, version_id)
        try:
            response = await client.put(destination, content=source_stream())
        except httpx.HTTPError as exc:
            raise ReplicaOperationError(f"destination node request failed: {exc}") from exc

        if response.status_code == 409:
            raise ReplicaOperationError("destination replica already exists")
        if response.status_code == 507:
            raise ReplicaOperationError("destination node is out of storage")
        if response.status_code >= 400:
            raise ReplicaOperationError(f"destination node rejected replica: HTTP {response.status_code}")

        try:
            body = response.json()
            size = int(body["size_bytes"])
        except (ValueError, KeyError, TypeError) as exc:
            raise ReplicaOperationError("destination node returned an invalid replica response") from exc

        try:
            await _verify_remote(client, destination_url, object_id, version_id)
        except ReplicaOperationError:
            try:
                await client.delete(destination)
            except httpx.HTTPError:
                pass
            raise

        return size

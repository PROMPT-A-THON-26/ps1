from __future__ import annotations

from collections.abc import AsyncIterator
from urllib.parse import quote

import httpx


class ReplicaOperationError(Exception):
    """Raised when a node-to-node replica operation fails."""


def _object_url(base_url: str, object_id: str, version_id: str) -> str:
    return (
        f"{base_url.rstrip('/')}/internal/v1/objects/"
        f"{quote(object_id, safe='')}/{quote(version_id, safe='')}"
    )


async def _verify_remote(
    client: httpx.AsyncClient,
    base_url: str,
    object_id: str,
    version_id: str,
) -> tuple[int, str]:
    url = f"{_object_url(base_url, object_id, version_id)}/verify"
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

    size_bytes = body.get("size_bytes")
    checksum = body.get("checksum")
    if (
        isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or size_bytes < 0
        or not isinstance(checksum, str)
        or len(checksum) != 64
        or checksum.lower() != checksum
        or any(character not in "0123456789abcdef" for character in checksum)
    ):
        raise ReplicaOperationError(
            "verification endpoint returned invalid size_bytes/checksum"
        )
    return size_bytes, checksum


async def stream_replica(
    source_url: str,
    object_id: str,
    version_id: str,
    *,
    timeout: float = 60.0,
) -> AsyncIterator[bytes]:
    """Stream one object from a trusted source node without buffering it."""
    url = _object_url(source_url, object_id, version_id)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client:
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
    destination = _object_url(destination_url, object_id, version_id)

    async def source_stream() -> AsyncIterator[bytes]:
        async for chunk in stream_replica(
            source_url,
            object_id,
            version_id,
            timeout=timeout,
        ):
            yield chunk

    async with httpx.AsyncClient(
        timeout=timeout,
        follow_redirects=False,
    ) as client:
        source_size, source_checksum = await _verify_remote(
            client, source_url, object_id, version_id
        )

        try:
            response = await client.put(destination, content=source_stream())
        except httpx.HTTPError as exc:
            # A request timeout can happen after the destination commits. Reconcile
            # the destination before treating the copy as failed.
            try:
                destination_size, destination_checksum = await _verify_remote(
                    client, destination_url, object_id, version_id
                )
            except ReplicaOperationError:
                raise ReplicaOperationError(
                    f"destination node request failed: {exc}"
                ) from exc
            if (
                destination_size == source_size
                and destination_checksum == source_checksum
            ):
                return destination_size
            raise ReplicaOperationError(
                f"destination node request failed and left a mismatched replica: {exc}"
            ) from exc

        if response.status_code == 409:
            raise ReplicaOperationError("destination replica already exists")
        if response.status_code == 507:
            raise ReplicaOperationError("destination node is out of storage")
        if response.status_code != 201:
            raise ReplicaOperationError(
                f"destination node rejected replica: HTTP {response.status_code}"
            )

        try:
            body = response.json()
        except ValueError as exc:
            raise ReplicaOperationError(
                "destination node returned invalid JSON"
            ) from exc

        response_object_id = body.get("object_id")
        response_version_id = body.get("version_id")
        raw_size = body.get("size_bytes")
        if (
            response_object_id != object_id
            or response_version_id != version_id
            or isinstance(raw_size, bool)
            or not isinstance(raw_size, int)
            or raw_size < 0
        ):
            raise ReplicaOperationError(
                "destination node returned an invalid replica response"
            )

        size = raw_size
        if size != source_size:
            try:
                await client.delete(destination)
            except httpx.HTTPError:
                pass
            raise ReplicaOperationError(
                "destination node reported a replica size different from the source"
            )

        try:
            destination_size, destination_checksum = await _verify_remote(
                client, destination_url, object_id, version_id
            )
            if destination_size != source_size or destination_checksum != source_checksum:
                raise ReplicaOperationError(
                    "destination replica does not match source checksum/size"
                )
        except ReplicaOperationError:
            try:
                await client.delete(destination)
            except httpx.HTTPError:
                pass
            raise

        return size

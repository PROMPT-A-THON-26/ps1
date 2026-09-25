"""HTTP client for the canonical Vault storage-node API.

The client deliberately does not mutate PostgreSQL metadata. It translates
storage-node HTTP outcomes into explicit control-plane exceptions and supports
streaming PUT/GET operations so large objects are never required to fit in
control-plane memory.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
from typing import Any
from urllib.parse import quote

import httpx


class StorageNodeClientError(Exception):
    """Base exception for storage-node communication/protocol failures."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        detail: Any = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.detail = detail
        self.request_id = request_id


class StorageNodeUnavailableError(StorageNodeClientError):
    """Raised when the storage node cannot currently be contacted."""


class StorageNodeInvalidRequestError(StorageNodeClientError):
    """Raised when the request violates the storage-node contract."""


class StorageObjectNotFoundError(StorageNodeClientError):
    """Raised when an object/version is absent on the target node."""


class StorageObjectAlreadyExistsError(StorageNodeClientError):
    """Raised when the target node already contains the object/version."""


class StorageNodeInsufficientCapacityError(StorageNodeClientError):
    """Raised when the target node cannot store the requested payload."""


class StorageNodeProtocolError(StorageNodeClientError):
    """Raised when a node returns an unexpected HTTP/protocol response."""


@dataclass(frozen=True, slots=True)
class StoredObject:
    object_id: str
    version_id: str
    size_bytes: int


@dataclass(frozen=True, slots=True)
class VerifiedObject:
    object_id: str
    version_id: str
    size_bytes: int
    checksum: str
    verified: bool


@dataclass(frozen=True, slots=True)
class NodeHealth:
    status: str
    node_id: str


@dataclass(frozen=True, slots=True)
class NodeStats:
    node_id: str
    capacity_bytes: int
    used_bytes: int
    free_bytes: int


@dataclass(frozen=True, slots=True)
class StorageNodeClientConfig:
    """Connection policy for one storage node."""

    address: str
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        address = self.address.strip().rstrip("/")
        if not address:
            raise ValueError("storage-node address must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        object.__setattr__(self, "address", address)


class StorageNodeClient:
    """Async client for the exact Part A storage-node HTTP contract."""

    def __init__(
        self,
        address: str | StorageNodeClientConfig,
        *,
        timeout_seconds: float = 30.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = (
            address
            if isinstance(address, StorageNodeClientConfig)
            else StorageNodeClientConfig(address, timeout_seconds)
        )
        self._client = client or httpx.AsyncClient(
            base_url=self.config.address,
            timeout=self.config.timeout_seconds,
            follow_redirects=False,
        )
        self._owns_client = client is None

    async def __aenter__(self) -> "StorageNodeClient":
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def put_object(
        self,
        object_id: str,
        version_id: str,
        data: bytes | AsyncIterable[bytes],
        *,
        request_id: str | None = None,
    ) -> StoredObject:
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")

        response = await self._request(
            "PUT",
            self._object_path(object_id, version_id),
            headers=self._headers(request_id, content_type="application/octet-stream"),
            content=data,
        )
        await self._raise_for_response(response)

        if response.status_code != httpx.codes.CREATED:
            raise StorageNodeProtocolError(
                f"Expected 201 from storage-node PUT, got {response.status_code}.",
                status_code=response.status_code,
                detail=response.text,
                request_id=response.headers.get("X-Request-ID"),
            )

        payload = self._json_object(response)
        result = StoredObject(
            object_id=self._required_string(payload, "object_id"),
            version_id=self._required_string(payload, "version_id"),
            size_bytes=self._required_nonnegative_int(payload, "size_bytes"),
        )
        if result.object_id != object_id or result.version_id != version_id:
            raise StorageNodeProtocolError(
                "Storage node returned identifiers different from the request.",
                status_code=response.status_code,
                detail=payload,
                request_id=response.headers.get("X-Request-ID"),
            )
        return result

    @asynccontextmanager
    async def stream_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> AsyncIterator[httpx.Response]:
        """Stream one object version without buffering it in the client."""
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")

        try:
            async with self._client.stream(
                "GET",
                self._object_path(object_id, version_id),
                headers=self._headers(request_id),
            ) as response:
                await self._raise_for_response(response)
                if response.status_code != httpx.codes.OK:
                    raise StorageNodeProtocolError(
                        f"Expected 200 from storage-node GET, got {response.status_code}.",
                        status_code=response.status_code,
                        detail=await self._response_text(response),
                        request_id=response.headers.get("X-Request-ID"),
                    )
                yield response
        except httpx.RequestError as exc:
            raise StorageNodeUnavailableError(
                f"Storage node {self.config.address} could not be reached: {exc}",
            ) from exc

    async def head_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> int:
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")

        response = await self._request(
            "HEAD",
            self._object_path(object_id, version_id),
            headers=self._headers(request_id),
        )
        await self._raise_for_response(response)

        if response.status_code != httpx.codes.OK:
            raise StorageNodeProtocolError(
                f"Expected 200 from storage-node HEAD, got {response.status_code}.",
                status_code=response.status_code,
                request_id=response.headers.get("X-Request-ID"),
            )

        raw_size = response.headers.get("content-length")
        if raw_size is None:
            raise StorageNodeProtocolError(
                "Storage-node HEAD response is missing Content-Length.",
                status_code=response.status_code,
                request_id=response.headers.get("X-Request-ID"),
            )
        try:
            size = int(raw_size)
        except ValueError as exc:
            raise StorageNodeProtocolError(
                "Storage-node HEAD returned a non-integer Content-Length.",
                status_code=response.status_code,
                detail=raw_size,
                request_id=response.headers.get("X-Request-ID"),
            ) from exc
        if size < 0:
            raise StorageNodeProtocolError(
                "Storage-node HEAD returned a negative Content-Length.",
                status_code=response.status_code,
                detail=raw_size,
                request_id=response.headers.get("X-Request-ID"),
            )
        return size

    async def delete_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> None:
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")

        response = await self._request(
            "DELETE",
            self._object_path(object_id, version_id),
            headers=self._headers(request_id),
        )
        await self._raise_for_response(response)

        if response.status_code != httpx.codes.NO_CONTENT:
            raise StorageNodeProtocolError(
                f"Expected 204 from storage-node DELETE, got {response.status_code}.",
                status_code=response.status_code,
                detail=await self._response_text(response),
                request_id=response.headers.get("X-Request-ID"),
            )

    async def verify_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> VerifiedObject:
        """Verify actual stored bytes and return their measured checksum/size."""
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")

        response = await self._request(
            "GET",
            f"{self._object_path(object_id, version_id)}/verify",
            headers=self._headers(request_id),
        )
        await self._raise_for_response(response)

        if response.status_code != httpx.codes.OK:
            raise StorageNodeProtocolError(
                f"Expected 200 from storage-node VERIFY, got {response.status_code}.",
                status_code=response.status_code,
                detail=await self._response_text(response),
                request_id=response.headers.get("X-Request-ID"),
            )

        payload = self._json_object(response)
        result = VerifiedObject(
            object_id=self._required_string(payload, "object_id"),
            version_id=self._required_string(payload, "version_id"),
            size_bytes=self._required_nonnegative_int(payload, "size_bytes"),
            checksum=self._required_checksum(payload, "checksum"),
            verified=bool(payload.get("verified", True)),
        )

        if result.object_id != object_id or result.version_id != version_id:
            raise StorageNodeProtocolError(
                "Storage-node VERIFY returned identifiers different from the request.",
                status_code=response.status_code,
                detail=payload,
                request_id=response.headers.get("X-Request-ID"),
            )
        return result

    async def health(self, *, request_id: str | None = None) -> NodeHealth:
        response = await self._request(
            "GET",
            "/internal/v1/health",
            headers=self._headers(request_id),
        )
        await self._raise_for_response(response)
        if response.status_code != httpx.codes.OK:
            raise StorageNodeProtocolError(
                f"Expected 200 from storage-node health, got {response.status_code}.",
                status_code=response.status_code,
                detail=await self._response_text(response),
                request_id=response.headers.get("X-Request-ID"),
            )

        payload = self._json_object(response)
        return NodeHealth(
            status=self._required_string(payload, "status"),
            node_id=self._required_string(payload, "node_id"),
        )

    async def stats(self, *, request_id: str | None = None) -> NodeStats:
        response = await self._request(
            "GET",
            "/internal/v1/stats",
            headers=self._headers(request_id),
        )
        await self._raise_for_response(response)
        if response.status_code != httpx.codes.OK:
            raise StorageNodeProtocolError(
                f"Expected 200 from storage-node stats, got {response.status_code}.",
                status_code=response.status_code,
                detail=await self._response_text(response),
                request_id=response.headers.get("X-Request-ID"),
            )

        payload = self._json_object(response)
        return NodeStats(
            node_id=self._required_string(payload, "node_id"),
            capacity_bytes=self._required_nonnegative_int(payload, "capacity_bytes"),
            used_bytes=self._required_nonnegative_int(payload, "used_bytes"),
            free_bytes=self._required_nonnegative_int(payload, "free_bytes"),
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            return await self._client.request(method, path, **kwargs)
        except httpx.RequestError as exc:
            raise StorageNodeUnavailableError(
                f"Storage node {self.config.address} could not be reached: {exc}",
            ) from exc

    async def _raise_for_response(self, response: httpx.Response) -> None:
        if response.is_success:
            return

        request_id = response.headers.get("X-Request-ID")
        detail = await self._response_json_or_text(response)

        if response.status_code == httpx.codes.BAD_REQUEST:
            raise StorageNodeInvalidRequestError(
                "Storage node rejected the request.",
                status_code=response.status_code,
                detail=detail,
                request_id=request_id,
            )
        if response.status_code == httpx.codes.NOT_FOUND:
            raise StorageObjectNotFoundError(
                "Object/version was not found on the storage node.",
                status_code=response.status_code,
                detail=detail,
                request_id=request_id,
            )
        if response.status_code == httpx.codes.CONFLICT:
            raise StorageObjectAlreadyExistsError(
                "Object/version already exists on the storage node.",
                status_code=response.status_code,
                detail=detail,
                request_id=request_id,
            )
        if response.status_code == 507:
            raise StorageNodeInsufficientCapacityError(
                "Storage node has insufficient capacity.",
                status_code=response.status_code,
                detail=detail,
                request_id=request_id,
            )

        raise StorageNodeProtocolError(
            f"Unexpected storage-node HTTP status {response.status_code}.",
            status_code=response.status_code,
            detail=detail,
            request_id=request_id,
        )

    @staticmethod
    def _json_object(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise StorageNodeProtocolError(
                "Storage node returned invalid JSON.",
                status_code=response.status_code,
                detail=response.text,
                request_id=response.headers.get("X-Request-ID"),
            ) from exc
        if not isinstance(payload, dict):
            raise StorageNodeProtocolError(
                "Storage node returned a non-object JSON payload.",
                status_code=response.status_code,
                detail=payload,
                request_id=response.headers.get("X-Request-ID"),
            )
        return payload

    @staticmethod
    async def _response_json_or_text(response: httpx.Response) -> Any:
        content = await response.aread()
        try:
            return json.loads(content)
        except (TypeError, ValueError):
            return content.decode("utf-8", errors="replace")

    @staticmethod
    async def _response_text(response: httpx.Response) -> str:
        content = await response.aread()
        return content.decode("utf-8", errors="replace")

    @staticmethod
    def _required_string(payload: dict[str, Any], name: str) -> str:
        value = payload.get(name)
        if not isinstance(value, str) or not value:
            raise StorageNodeProtocolError(
                f"Storage node response is missing a valid '{name}' field.",
                detail=payload,
            )
        return value

    @staticmethod
    def _required_nonnegative_int(payload: dict[str, Any], name: str) -> int:
        value = payload.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise StorageNodeProtocolError(
                f"Storage node response is missing a valid non-negative '{name}' field.",
                detail=payload,
            )
        return value

    @staticmethod
    def _required_checksum(payload: dict[str, Any], name: str) -> str:
        value = payload.get(name)
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdefABCDEF" for character in value)
        ):
            raise StorageNodeProtocolError(
                "Storage node response is missing a valid SHA-256 checksum.",
                detail=payload,
            )
        return value.lower()

    @staticmethod
    def _headers(
        request_id: str | None,
        *,
        content_type: str | None = None,
    ) -> dict[str, str]:
        headers: dict[str, str] = {}
        if request_id:
            headers["X-Request-ID"] = request_id
        if content_type:
            headers["Content-Type"] = content_type
        return headers

    @staticmethod
    def _object_path(object_id: str, version_id: str) -> str:
        StorageNodeClient._validate_identifier(object_id, "object_id")
        StorageNodeClient._validate_identifier(version_id, "version_id")
        return (
            "/internal/v1/objects/"
            f"{quote(object_id, safe='')}/"
            f"{quote(version_id, safe='')}"
        )

    @staticmethod
    def _validate_identifier(value: str, field_name: str) -> None:
        if (
            not isinstance(value, str)
            or not value
            or value in {".", ".."}
            or "/" in value
            or "\\" in value
            or "\x00" in value
        ):
            raise ValueError(f"Invalid {field_name}")

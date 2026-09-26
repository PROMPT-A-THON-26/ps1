"""Async HTTP client for Vault's canonical storage-node API.

This module is the control-plane boundary to Part A. It keeps transport,
timeouts, retries, request tracing, response validation, and error mapping in
one place so higher-level replication/repair code can remain policy-focused.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
import json
import random
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from common.ids import new_request_id, validate_request_id
from common.settings import settings


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
    """The node cannot currently be reached or timed out."""


class StorageNodeInvalidRequestError(StorageNodeClientError):
    """The request violates the storage-node contract."""


class StorageObjectNotFoundError(StorageNodeClientError):
    """The requested object/version is absent on the node."""


class StorageObjectAlreadyExistsError(StorageNodeClientError):
    """The requested object/version already exists on the node."""


class StorageNodeInsufficientCapacityError(StorageNodeClientError):
    """The node cannot satisfy the requested storage capacity."""


class StorageNodeProtocolError(StorageNodeClientError):
    """The node returned a malformed or unsupported response."""


class StorageNodeIntegrityError(StorageNodeClientError):
    """The node successfully inspected an object but found integrity corruption."""


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
    valid: bool = True


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
class RetryPolicy:
    """Retry policy for safe/idempotent operations."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.2
    max_delay_seconds: float = 2.0
    jitter_ratio: float = 0.25

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must be non-negative")
        if self.max_delay_seconds < self.base_delay_seconds:
            raise ValueError("max_delay_seconds must be >= base_delay_seconds")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")


@dataclass(frozen=True, slots=True)
class StorageNodeTimeouts:
    """Operation-specific transport timeouts."""

    connect_seconds: float = 5.0
    health_seconds: float = 2.0
    stats_seconds: float = 3.0
    head_seconds: float = 5.0
    read_seconds: float = 30.0
    write_seconds: float = 60.0
    verify_seconds: float = 30.0
    delete_seconds: float = 10.0
    pool_seconds: float = 5.0

    def __post_init__(self) -> None:
        values = (
            ("connect_seconds", self.connect_seconds),
            ("health_seconds", self.health_seconds),
            ("stats_seconds", self.stats_seconds),
            ("head_seconds", self.head_seconds),
            ("read_seconds", self.read_seconds),
            ("write_seconds", self.write_seconds),
            ("verify_seconds", self.verify_seconds),
            ("delete_seconds", self.delete_seconds),
            ("pool_seconds", self.pool_seconds),
        )
        for name, value in values:
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")

    def for_operation(self, operation: str) -> httpx.Timeout:
        read = {
            "health": self.health_seconds,
            "stats": self.stats_seconds,
            "head": self.head_seconds,
            "read": self.read_seconds,
            "write": self.write_seconds,
            "verify": self.verify_seconds,
            "delete": self.delete_seconds,
        }.get(operation)
        if read is None:
            raise ValueError(f"unknown operation: {operation}")
        return httpx.Timeout(
            connect=self.connect_seconds,
            read=read,
            write=self.write_seconds if operation == "write" else read,
            pool=self.pool_seconds,
        )


@dataclass(frozen=True, slots=True)
class StorageNodeClientConfig:
    """Connection and transport policy for one storage node."""

    address: str
    timeout_seconds: float | None = None
    timeouts: StorageNodeTimeouts | None = None
    retry_policy: RetryPolicy = RetryPolicy()
    max_connections: int = 100
    max_keepalive_connections: int = 20

    def __post_init__(self) -> None:
        address = self.address.strip().rstrip("/")
        if not address:
            raise ValueError("storage-node address must not be empty")
        parsed = urlparse(address)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("storage-node address must be an absolute http(s) URL")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("storage-node address must not contain embedded credentials")
        object.__setattr__(self, "address", address)

        if self.timeout_seconds is not None and self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        if self.max_connections < 1:
            raise ValueError("max_connections must be at least 1")
        if not 1 <= self.max_keepalive_connections <= self.max_connections:
            raise ValueError(
                "max_keepalive_connections must be between 1 and max_connections"
            )

        if self.timeouts is None:
            timeout = self.timeout_seconds
            object.__setattr__(
                self,
                "timeouts",
                StorageNodeTimeouts(
                    connect_seconds=timeout or 5.0,
                    health_seconds=timeout or 2.0,
                    stats_seconds=timeout or 3.0,
                    head_seconds=timeout or 5.0,
                    read_seconds=timeout or 30.0,
                    write_seconds=timeout or 60.0,
                    verify_seconds=timeout or 30.0,
                    delete_seconds=timeout or 10.0,
                    pool_seconds=timeout or 5.0,
                ),
            )


class StorageNodeClient:
    """Async client for the exact Part A storage-node HTTP contract."""

    RETRYABLE_STATUS_CODES = frozenset({502, 503, 504})
    SAFE_RETRY_OPERATIONS = frozenset(
        {"health", "stats", "head", "read", "verify", "delete"}
    )

    def __init__(
        self,
        address: str | StorageNodeClientConfig,
        *,
        timeout_seconds: float | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds is None and not isinstance(address, StorageNodeClientConfig):
            timeout_seconds = settings.storage_request_timeout_seconds
        self.config = (
            address
            if isinstance(address, StorageNodeClientConfig)
            else StorageNodeClientConfig(address, timeout_seconds=timeout_seconds)
        )
        self._client = client or httpx.AsyncClient(
            base_url=self.config.address,
            timeout=self.config.timeouts.for_operation("read"),
            limits=httpx.Limits(
                max_connections=self.config.max_connections,
                max_keepalive_connections=self.config.max_keepalive_connections,
            ),
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
        """Write one immutable object version.

        PUT is intentionally not automatically retried because an HTTP timeout
        can occur after the storage node has already committed the object. A
        higher-level replication operation can safely reconcile such a case by
        calling VERIFY before deciding whether to retry.
        """
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")
        rid = self._request_id(request_id)

        response = await self._request(
            "PUT",
            self._object_path(object_id, version_id),
            operation="write",
            headers=self._headers(rid, content_type="application/octet-stream"),
            content=data,
            retry=False,
        )
        try:
            await self._raise_for_response(response)
            if response.status_code != httpx.codes.CREATED:
                raise self._protocol_status(
                    response,
                    f"Expected 201 from storage-node PUT, got {response.status_code}.",
                    rid,
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
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            return result
        finally:
            await response.aclose()

    @asynccontextmanager
    async def stream_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> AsyncIterator[httpx.Response]:
        """Open a streaming GET.

        Only connection establishment and HTTP response status are retried. A
        transport error raised while the caller consumes response bytes is not
        retried, because the client cannot safely replay a partially delivered
        stream without caller-level range/resume semantics.
        """
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")
        rid = self._request_id(request_id)

        for attempt in range(1, self.config.retry_policy.max_attempts + 1):
            context = self._client.stream(
                "GET",
                self._object_path(object_id, version_id),
                headers=self._headers(rid),
                timeout=self.config.timeouts.for_operation("read"),
            )
            try:
                try:
                    response = await context.__aenter__()
                except httpx.TimeoutException as exc:
                    if attempt >= self.config.retry_policy.max_attempts:
                        raise StorageNodeUnavailableError(
                            f"Storage node {self.config.address} timed out during GET.",
                            request_id=rid,
                        ) from exc
                    await self._sleep_before_retry(attempt)
                    continue
                except httpx.RequestError as exc:
                    if attempt >= self.config.retry_policy.max_attempts:
                        raise StorageNodeUnavailableError(
                            f"Storage node {self.config.address} could not be reached.",
                            request_id=rid,
                        ) from exc
                    await self._sleep_before_retry(attempt)
                    continue

                if response.status_code in self.RETRYABLE_STATUS_CODES and attempt < self.config.retry_policy.max_attempts:
                    await context.__aexit__(None, None, None)
                    await self._sleep_before_retry(attempt)
                    continue

                await self._raise_for_response(response)
                if response.status_code != httpx.codes.OK:
                    raise self._protocol_status(
                        response,
                        f"Expected 200 from storage-node GET, got {response.status_code}.",
                        rid,
                    )

                try:
                    yield response
                finally:
                    await context.__aexit__(None, None, None)
                return
            except StorageNodeClientError:
                # _raise_for_response closes protocol-error responses. Keep the
                # context manager consistent for any error that escaped before
                # the caller received the response.
                try:
                    await context.__aexit__(None, None, None)
                except Exception:
                    pass
                raise
            except Exception:
                try:
                    await context.__aexit__(None, None, None)
                except Exception:
                    pass
                raise

    async def head_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> int:
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")
        rid = self._request_id(request_id)
        response = await self._request(
            "HEAD",
            self._object_path(object_id, version_id),
            operation="head",
            headers=self._headers(rid),
            retry=True,
        )
        try:
            await self._raise_for_response(response)
            if response.status_code != httpx.codes.OK:
                raise self._protocol_status(
                    response,
                    f"Expected 200 from storage-node HEAD, got {response.status_code}.",
                    rid,
                )

            raw_size = response.headers.get("content-length")
            if raw_size is None:
                raise StorageNodeProtocolError(
                    "Storage-node HEAD response is missing Content-Length.",
                    status_code=response.status_code,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            try:
                size = int(raw_size)
            except ValueError as exc:
                raise StorageNodeProtocolError(
                    "Storage-node HEAD returned a non-integer Content-Length.",
                    status_code=response.status_code,
                    detail=raw_size,
                    request_id=response.headers.get("X-Request-ID", rid),
                ) from exc
            if size < 0:
                raise StorageNodeProtocolError(
                    "Storage-node HEAD returned a negative Content-Length.",
                    status_code=response.status_code,
                    detail=raw_size,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            return size
        finally:
            await response.aclose()

    async def delete_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
    ) -> None:
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")
        rid = self._request_id(request_id)
        response = await self._request(
            "DELETE",
            self._object_path(object_id, version_id),
            operation="delete",
            headers=self._headers(rid),
            retry=True,
        )
        try:
            if response.status_code == httpx.codes.NOT_FOUND:
                # DELETE is idempotent: the desired state is already achieved
                # when the object version is absent.
                await response.aclose()
                return
            await self._raise_for_response(response)
            if response.status_code != httpx.codes.NO_CONTENT:
                raise self._protocol_status(
                    response,
                    f"Expected 204 from storage-node DELETE, got {response.status_code}.",
                    rid,
                )
        finally:
            await response.aclose()

    async def verify_object(
        self,
        object_id: str,
        version_id: str,
        *,
        request_id: str | None = None,
        raise_on_invalid: bool = True,
    ) -> VerifiedObject:
        """Verify stored bytes and return measured checksum/size."""
        if type(raise_on_invalid) is not bool:
            raise TypeError("raise_on_invalid must be a bool")
        self._validate_identifier(object_id, "object_id")
        self._validate_identifier(version_id, "version_id")
        rid = self._request_id(request_id)
        response = await self._request(
            "GET",
            f"{self._object_path(object_id, version_id)}/verify",
            operation="verify",
            headers=self._headers(rid),
            retry=True,
        )
        try:
            await self._raise_for_response(response)
            if response.status_code != httpx.codes.OK:
                raise self._protocol_status(
                    response,
                    f"Expected 200 from storage-node VERIFY, got {response.status_code}.",
                    rid,
                )

            payload = self._json_object(response)

            if "verified" not in payload:
                raise StorageNodeProtocolError(
                    "Storage node VERIFY response must include an explicit verified field.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            verified = payload["verified"]
            if type(verified) is not bool or not verified:
                raise StorageNodeProtocolError(
                    "Storage node VERIFY response must explicitly report verified=true.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )

            valid = payload.get("valid", True)
            if type(valid) is not bool:
                raise StorageNodeProtocolError(
                    "Storage node VERIFY response has an invalid 'valid' field.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )

            result = VerifiedObject(
                object_id=self._required_string(payload, "object_id"),
                version_id=self._required_string(payload, "version_id"),
                size_bytes=self._required_nonnegative_int(payload, "size_bytes"),
                checksum=self._required_checksum(payload, "checksum"),
                verified=True,
                valid=valid,
            )
            if result.object_id != object_id or result.version_id != version_id:
                raise StorageNodeProtocolError(
                    "Storage-node VERIFY returned identifiers different from the request.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            if not result.valid and raise_on_invalid:
                raise StorageNodeIntegrityError(
                    "Storage node reported that stored data failed integrity verification.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            return result
        finally:
            await response.aclose()

    async def health(self, *, request_id: str | None = None) -> NodeHealth:
        rid = self._request_id(request_id)
        response = await self._request(
            "GET",
            "/internal/v1/health",
            operation="health",
            headers=self._headers(rid),
            retry=True,
        )
        try:
            await self._raise_for_response(response)
            if response.status_code != httpx.codes.OK:
                raise self._protocol_status(
                    response,
                    f"Expected 200 from storage-node health, got {response.status_code}.",
                    rid,
                )
            payload = self._json_object(response)
            return NodeHealth(
                status=self._required_string(payload, "status"),
                node_id=self._required_string(payload, "node_id"),
            )
        finally:
            await response.aclose()

    async def stats(self, *, request_id: str | None = None) -> NodeStats:
        rid = self._request_id(request_id)
        response = await self._request(
            "GET",
            "/internal/v1/stats",
            operation="stats",
            headers=self._headers(rid),
            retry=True,
        )
        try:
            await self._raise_for_response(response)
            if response.status_code != httpx.codes.OK:
                raise self._protocol_status(
                    response,
                    f"Expected 200 from storage-node stats, got {response.status_code}.",
                    rid,
                )
            payload = self._json_object(response)
            result = NodeStats(
                node_id=self._required_string(payload, "node_id"),
                capacity_bytes=self._required_nonnegative_int(payload, "capacity_bytes"),
                used_bytes=self._required_nonnegative_int(payload, "used_bytes"),
                free_bytes=self._required_nonnegative_int(payload, "free_bytes"),
            )
            if result.used_bytes > result.capacity_bytes:
                raise StorageNodeProtocolError(
                    "Storage-node STATS reports used_bytes above capacity_bytes.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            if result.free_bytes > result.capacity_bytes:
                raise StorageNodeProtocolError(
                    "Storage-node STATS reports free_bytes above capacity_bytes.",
                    status_code=response.status_code,
                    detail=payload,
                    request_id=response.headers.get("X-Request-ID", rid),
                )
            return result
        finally:
            await response.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        retry: bool,
        **kwargs: Any,
    ) -> httpx.Response:
        attempts = (
            self.config.retry_policy.max_attempts
            if retry and operation in self.SAFE_RETRY_OPERATIONS
            else 1
        )

        for attempt in range(1, attempts + 1):
            try:
                response = await self._client.request(
                    method,
                    path,
                    timeout=self.config.timeouts.for_operation(operation),
                    **kwargs,
                )
                if (
                    response.status_code in self.RETRYABLE_STATUS_CODES
                    and attempt < attempts
                ):
                    await response.aclose()
                    await self._sleep_before_retry(attempt)
                    continue
                return response
            except httpx.TimeoutException as exc:
                if attempt >= attempts:
                    raise StorageNodeUnavailableError(
                        f"Storage node {self.config.address} timed out during {operation}.",
                        request_id=kwargs.get("headers", {}).get("X-Request-ID"),
                    ) from exc
                await self._sleep_before_retry(attempt)
            except httpx.RequestError as exc:
                if attempt >= attempts:
                    raise StorageNodeUnavailableError(
                        f"Storage node {self.config.address} could not be reached during {operation}.",
                        request_id=kwargs.get("headers", {}).get("X-Request-ID"),
                    ) from exc
                await self._sleep_before_retry(attempt)

        raise StorageNodeUnavailableError(
            f"Storage node request failed during {operation}.",
            request_id=kwargs.get("headers", {}).get("X-Request-ID"),
        )

    async def _raise_for_response(self, response: httpx.Response) -> None:
        if response.is_success:
            return

        request_id = response.headers.get("X-Request-ID")
        detail = await self._response_json_or_text(response)
        try:
            if response.status_code in {400, 422}:
                raise StorageNodeInvalidRequestError(
                    "Storage node rejected the request.",
                    status_code=response.status_code,
                    detail=detail,
                    request_id=request_id,
                )
            if response.status_code in self.RETRYABLE_STATUS_CODES:
                raise StorageNodeUnavailableError(
                    f"Storage node returned transient HTTP {response.status_code}.",
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

            raise self._protocol_status(
                response,
                f"Unexpected storage-node HTTP status {response.status_code}.",
                request_id,
            )
        finally:
            await response.aclose()

    @staticmethod
    def _protocol_status(
        response: httpx.Response,
        message: str,
        request_id: str | None,
    ) -> StorageNodeProtocolError:
        return StorageNodeProtocolError(
            message,
            status_code=response.status_code,
            detail=response.text,
            request_id=response.headers.get("X-Request-ID", request_id),
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
    def _request_id(request_id: str | None) -> str:
        if request_id is None:
            return new_request_id()
        return validate_request_id(request_id.strip())

    @staticmethod
    def _headers(
        request_id: str,
        *,
        content_type: str | None = None,
    ) -> dict[str, str]:
        headers = {"X-Request-ID": request_id}
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

    async def _sleep_before_retry(self, attempt: int) -> None:
        base = min(
            self.config.retry_policy.max_delay_seconds,
            self.config.retry_policy.base_delay_seconds * (2 ** (attempt - 1)),
        )
        if base <= 0:
            return
        ratio = self.config.retry_policy.jitter_ratio
        delay = random.uniform(base * (1 - ratio), base * (1 + ratio))
        delay = min(delay, self.config.retry_policy.max_delay_seconds)
        await asyncio.sleep(delay)

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

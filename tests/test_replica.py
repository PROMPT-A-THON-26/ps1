import asyncio
import hashlib

import httpx
import pytest

from storage.replica import ReplicaOperationError, copy_replica


def run(coro):
    return asyncio.run(coro)


def test_copy_replica_rejects_corrupt_source(monkeypatch):
    async def handler(request):
        return httpx.Response(200, json={"valid": False, "errors": ["checksum mismatch"]})

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    with pytest.raises(ReplicaOperationError, match="checksum mismatch"):
        run(copy_replica("http://source", "http://destination", "obj", "ver"))


def test_copy_replica_rejects_existing_destination(monkeypatch):
    async def handler(request):
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "valid": True,
                    "size_bytes": 8,
                    "checksum": hashlib.sha256(b"abcdefgh").hexdigest(),
                    "errors": [],
                },
            )
        return httpx.Response(409)

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    with pytest.raises(ReplicaOperationError, match="already exists"):
        run(copy_replica("http://source", "http://destination", "obj", "ver"))


def test_copy_replica_rejects_destination_capacity(monkeypatch):
    async def handler(request):
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "valid": True,
                    "size_bytes": 8,
                    "checksum": hashlib.sha256(b"abcdefgh").hexdigest(),
                    "errors": [],
                },
            )
        return httpx.Response(507)

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    with pytest.raises(ReplicaOperationError, match="out of storage"):
        run(copy_replica("http://source", "http://destination", "obj", "ver"))


def test_copy_replica_streams_and_verifies_source_and_destination(monkeypatch):
    requests = []
    payload = b"abcdefgh"

    async def handler(request):
        requests.append((request.method, request.url.host, request.url.path))
        if request.method == "GET" and request.url.path.endswith("/verify"):
            return httpx.Response(
                200,
                json={
                    "valid": True,
                    "size_bytes": len(payload),
                    "checksum": hashlib.sha256(payload).hexdigest(),
                    "errors": [],
                },
            )
        if request.method == "GET":
            return httpx.Response(200, content=payload)
        if request.method == "PUT":
            body = await request.aread()
            assert body == payload
            return httpx.Response(201, json={"object_id": "obj", "version_id": "ver", "size_bytes": len(body)})
        raise AssertionError(request.method)

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    assert run(copy_replica("http://source", "http://destination", "obj", "ver")) == len(payload)
    assert [item[0] for item in requests] == ["GET", "GET", "PUT", "GET"]


def test_copy_replica_rolls_back_when_destination_verification_fails(monkeypatch):
    deleted = []

    async def handler(request):
        if request.method == "GET" and request.url.host == "source":
            return httpx.Response(
                200,
                json={
                    "valid": True,
                    "size_bytes": len(b"source-data"),
                    "checksum": hashlib.sha256(b"source-data").hexdigest(),
                    "errors": [],
                },
            )
        if request.method == "GET":
            return httpx.Response(200, json={"valid": False, "errors": ["corrupt replica"]})
        if request.method == "PUT":
            body = await request.aread()
            return httpx.Response(201, json={"object_id": "obj", "version_id": "ver", "size_bytes": len(body)})
        if request.method == "DELETE":
            deleted.append(request.url.path)
            return httpx.Response(204)
        raise AssertionError(request.method)

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    with pytest.raises(ReplicaOperationError, match="corrupt replica"):
        run(copy_replica("http://source", "http://destination", "obj", "ver"))

    assert deleted == ["/internal/v1/objects/obj/ver"]

def test_copy_replica_rejects_self_consistent_wrong_destination(monkeypatch):
    deleted = []
    source_payload = b"abcdefgh"

    async def handler(request):
        if request.method == "GET" and request.url.host == "source":
            if request.url.path.endswith("/verify"):
                return httpx.Response(
                    200,
                    json={
                        "valid": True,
                        "size_bytes": len(source_payload),
                        "checksum": hashlib.sha256(source_payload).hexdigest(),
                        "errors": [],
                    },
                )
            return httpx.Response(200, content=source_payload)

        if request.method == "PUT":
            await request.aread()
            return httpx.Response(201, json={"object_id": "obj", "version_id": "ver", "size_bytes": len(b"WRONG!!!")})

        if request.method == "GET" and request.url.host == "destination":
            return httpx.Response(
                200,
                json={
                    "valid": True,
                    "size_bytes": len(b"WRONG!!!"),
                    "checksum": hashlib.sha256(b"WRONG!!!").hexdigest(),
                    "errors": [],
                },
            )

        if request.method == "DELETE":
            deleted.append(request.url.path)
            return httpx.Response(204)

        raise AssertionError(request.method)

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    with pytest.raises(ReplicaOperationError, match="does not match source checksum/size"):
        run(copy_replica("http://source", "http://destination", "obj", "ver"))

    assert deleted == ["/internal/v1/objects/obj/ver"]


def test_copy_replica_quotes_object_and_version_identifiers(monkeypatch):
    requests = []

    async def handler(request):
        requests.append(request)
        if request.method == "GET" and request.url.path.endswith("/verify"):
            return httpx.Response(
                200,
                json={
                    "valid": True,
                    "size_bytes": 4,
                    "checksum": hashlib.sha256(b"data").hexdigest(),
                    "errors": [],
                },
            )
        if request.method == "GET":
            return httpx.Response(200, content=b"data")
        if request.method == "PUT":
            body = await request.aread()
            return httpx.Response(
                201,
                json={
                    "object_id": "object id",
                    "version_id": "version id",
                    "size_bytes": len(body),
                },
            )
        raise AssertionError(request.method)

    transport = httpx.MockTransport(handler)
    original = httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    monkeypatch.setattr("storage.replica.httpx.AsyncClient", factory)

    assert run(
        copy_replica(
            "http://source",
            "http://destination",
            "object id",
            "version id",
        )
    ) == 4
    assert any(request.url.path == "/internal/v1/objects/object%20id/version%20id" for request in requests)

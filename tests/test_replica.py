import asyncio

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
            return httpx.Response(200, json={"valid": True, "errors": []})
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
            return httpx.Response(200, json={"valid": True, "errors": []})
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
            return httpx.Response(200, json={"valid": True, "errors": []})
        if request.method == "GET":
            return httpx.Response(200, content=payload)
        if request.method == "PUT":
            body = await request.aread()
            assert body == payload
            return httpx.Response(201, json={"size_bytes": len(body)})
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
            return httpx.Response(200, json={"valid": True, "errors": []})
        if request.method == "GET":
            return httpx.Response(200, json={"valid": False, "errors": ["corrupt replica"]})
        if request.method == "PUT":
            body = await request.aread()
            return httpx.Response(201, json={"size_bytes": len(body)})
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

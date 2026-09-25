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

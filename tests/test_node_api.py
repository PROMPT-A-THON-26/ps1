from fastapi.testclient import TestClient

from storage import node_server


def test_health():
    with TestClient(node_server.app) as client:
        response = client.get("/internal/v1/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
        assert response.json()["node_id"] == node_server.config.node_id


def test_stats():
    with TestClient(node_server.app) as client:
        response = client.get("/internal/v1/stats")
        assert response.status_code == 200
        body = response.json()
        assert body["node_id"] == node_server.config.node_id
        assert body["capacity_bytes"] > 0
        assert body["used_bytes"] >= 0


def test_object_lifecycle():
    with TestClient(node_server.app) as client:
        put = client.put("/internal/v1/objects/obj-test/ver-1", content=b"vault-data")
        assert put.status_code == 201

        head = client.head("/internal/v1/objects/obj-test/ver-1")
        assert head.status_code == 200
        assert head.headers["content-length"] == "10"

        get = client.get("/internal/v1/objects/obj-test/ver-1")
        assert get.status_code == 200
        assert get.content == b"vault-data"

        duplicate = client.put("/internal/v1/objects/obj-test/ver-1", content=b"other")
        assert duplicate.status_code == 409

        delete = client.delete("/internal/v1/objects/obj-test/ver-1")
        assert delete.status_code == 204

        missing = client.get("/internal/v1/objects/obj-test/ver-1")
        assert missing.status_code == 404

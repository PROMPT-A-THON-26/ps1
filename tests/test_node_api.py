from fastapi.testclient import TestClient

from storage import node_server
from storage.storage_engine import StorageEngine


def client_for(tmp_path):
    node_server.engine = StorageEngine(tmp_path, 1024 * 1024, chunk_size_bytes=4)
    return TestClient(node_server.app)


def test_health(tmp_path):
    with client_for(tmp_path) as client:
        response = client.get("/internal/v1/health")
        assert response.status_code == 200
        assert response.json() == {
            "status": "healthy",
            "node_id": node_server.config.node_id,
        }


def test_stats(tmp_path):
    with client_for(tmp_path) as client:
        response = client.get("/internal/v1/stats")
        assert response.status_code == 200
        body = response.json()
        assert body["node_id"] == node_server.config.node_id
        assert body["capacity_bytes"] == 1024 * 1024
        assert body["used_bytes"] >= 0


def test_object_lifecycle(tmp_path):
    with client_for(tmp_path) as client:
        payload = b"vault-data"
        put = client.put(
            "/internal/v1/objects/obj-test/ver-1",
            content=payload,
        )
        assert put.status_code == 201
        assert put.json() == {
            "object_id": "obj-test",
            "version_id": "ver-1",
            "size_bytes": len(payload),
        }

        head = client.head("/internal/v1/objects/obj-test/ver-1")
        assert head.status_code == 200
        assert head.headers["content-length"] == str(len(payload))

        get = client.get("/internal/v1/objects/obj-test/ver-1")
        assert get.status_code == 200
        assert get.content == payload

        duplicate = client.put(
            "/internal/v1/objects/obj-test/ver-1",
            content=b"other",
        )
        assert duplicate.status_code == 409

        delete = client.delete("/internal/v1/objects/obj-test/ver-1")
        assert delete.status_code == 204

        missing = client.get("/internal/v1/objects/obj-test/ver-1")
        assert missing.status_code == 404


def test_large_object_is_stored_as_multiple_chunks(tmp_path):
    with client_for(tmp_path) as client:
        payload = b"0123456789" * 3
        put = client.put(
            "/internal/v1/objects/large/ver-1",
            content=payload,
        )
        assert put.status_code == 201
        assert put.json()["size_bytes"] == len(payload)

        version_dir = tmp_path / "objects" / "large" / "ver-1"
        assert len(list(version_dir.glob("chunk-*"))) == 8

        streamed = client.get("/internal/v1/objects/large/ver-1")
        assert streamed.status_code == 200
        assert streamed.headers["content-length"] == str(len(payload))
        assert streamed.content == payload


def test_invalid_id_returns_bad_request(tmp_path):
    with client_for(tmp_path) as client:
        response = client.get("/internal/v1/objects/%2E%2E/ver-1")
        assert response.status_code == 400


def test_verify_endpoint_detects_corruption(tmp_path):
    with client_for(tmp_path) as client:
        payload = b"abcdefghij"
        assert client.put("/internal/v1/objects/obj-1/ver-1", content=payload).status_code == 201
        version_dir = tmp_path / "objects" / "obj-1" / "ver-1"
        (version_dir / "chunk-000001").write_bytes(b"XXXX")
        response = client.get("/internal/v1/objects/obj-1/ver-1/verify")
        assert response.status_code == 200
        body = response.json()
        assert body["valid"] is False
        assert 1 in body["corrupt_chunks"]

def test_verify_endpoint_missing_object(tmp_path):
    with client_for(tmp_path) as client:
        response = client.get("/internal/v1/objects/missing/ver-1/verify")
        assert response.status_code == 404


def test_verify_endpoint_reports_corrupt_metadata(tmp_path):
    import json
    with client_for(tmp_path) as client:
        payload = b"abcdefgh"
        assert client.put("/internal/v1/objects/obj-1/ver-1", content=payload).status_code == 201
        metadata_path = tmp_path / "objects" / "obj-1" / "ver-1" / "metadata.json"
        metadata = json.loads(metadata_path.read_text())
        metadata["checksum"] = "0" * 64
        metadata_path.write_text(json.dumps(metadata))
        response = client.get("/internal/v1/objects/obj-1/ver-1/verify")
        assert response.status_code == 200
        body = response.json()
        assert body["valid"] is False
        assert "object checksum mismatch" in body["errors"]

def test_verify_endpoint_rejects_invalid_id(tmp_path):
    with client_for(tmp_path) as client:
        response = client.get("/internal/v1/objects/%2E%2E/ver-1/verify")
        assert response.status_code == 400

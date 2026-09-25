"""Application service layer for public Vault gateway operations."""

from __future__ import annotations

from dataclasses import asdict
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import NodeState, ObjectState, ReplicaState, VersionState
from common.errors import ObjectNotFound
from metadata.models import Object, Replica, StorageNode, Version


class GatewayService:
    """Keep gateway routes free of scattered SQL and transport policy."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def list_objects(self) -> list[Object]:
        return list(self.session.scalars(select(Object).order_by(Object.name)).all())

    def get_object(self, name: str) -> Object:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("object name must not be empty")
        obj = self.session.scalar(select(Object).where(Object.name == name.strip()))
        if obj is None:
            raise ObjectNotFound(name.strip())
        return obj

    def object_metadata(self, name: str) -> dict:
        obj = self.get_object(name)
        current = None
        if obj.current_version_id is not None:
            current = self.session.scalar(
                select(Version).where(Version.version_id == obj.current_version_id)
            )
            if current is None:
                raise ValueError("object current_version_id references a missing version")
        return {
            "object_id": str(obj.object_id),
            "name": obj.name,
            "state": obj.state.value,
            "current_version": None if current is None else current.version_number,
            "current_version_id": None if current is None else str(current.version_id),
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
        }

    def versions(self, name: str) -> list[dict]:
        obj = self.get_object(name)
        versions = self.session.scalars(
            select(Version).where(Version.object_id == obj.object_id).order_by(Version.version_number)
        ).all()
        result = []
        for version in versions:
            healthy = self.session.scalar(
                select(func.count(Replica.replica_id)).where(
                    Replica.version_id == version.version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
            )
            result.append({
                "version_id": str(version.version_id),
                "version_number": version.version_number,
                "size_bytes": version.size_bytes,
                "checksum": version.checksum,
                "state": version.state.value,
                "healthy_replicas": int(healthy or 0),
                "created_at": version.created_at,
                "committed_at": version.committed_at,
            })
        return result

    def list_nodes(self) -> list[StorageNode]:
        return list(self.session.scalars(select(StorageNode).order_by(StorageNode.node_id)).all())

    def get_node(self, node_id: str) -> StorageNode:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        node = self.session.scalar(select(StorageNode).where(StorageNode.node_id == node_id.strip()))
        if node is None:
            raise ObjectNotFound(node_id.strip())
        return node

    def health(self) -> dict:
        nodes = self.list_nodes()
        counts = {state.value: 0 for state in NodeState}
        for node in nodes:
            counts[node.status.value] += 1
        return {"status": "ok", "nodes": len(nodes), "node_states": counts}

    def head(self, name: str) -> tuple[Object, Version | None]:
        obj = self.get_object(name)
        if obj.current_version_id is None:
            return obj, None
        version = self.session.scalar(select(Version).where(Version.version_id == obj.current_version_id))
        if version is None:
            raise ValueError("object current_version_id references a missing version")
        if version.state is not VersionState.COMMITTED:
            return obj, None
        return obj, version

    def ensure_not_deleted(self, name: str) -> Object:
        obj = self.get_object(name)
        if obj.state is ObjectState.DELETED:
            raise ObjectNotFound(name.strip())
        return obj

"""Transactional metadata/domain operations for the Vault control plane."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import NodeState, ObjectState, ReplicaState, VersionState
from common.errors import (
    ChecksumMismatch,
    InvalidState,
    ObjectAlreadyExists,
    ObjectNotFound,
    VersionConflict,
)
from common.ids import new_uuid

from .models import Object, Replica, StorageNode, Version


def _validate_checksum(checksum: str) -> str:
    if not isinstance(checksum, str) or len(checksum) != 64:
        raise ValueError("checksum must be a 64-character SHA-256 hex digest")
    normalized = checksum.lower()
    if any(ch not in "0123456789abcdef" for ch in normalized):
        raise ValueError("checksum must contain only hexadecimal characters")
    return normalized


class MetadataManager:
    """Keep object/version/replica/node state transactional and internally consistent."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_object(self, name: str) -> Optional[Object]:
        if not isinstance(name, str) or not name:
            raise ValueError("object name must not be empty")
        with self.session.begin():
            return self.session.scalar(select(Object).where(Object.name == name))

    def get_object_or_raise(self, name: str) -> Object:
        obj = self.get_object(name)
        if obj is None:
            raise ObjectNotFound(name)
        return obj

    def create_object(self, name: str) -> Object:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("object name must not be empty")
        normalized_name = name.strip()
        with self.session.begin():
            existing = self.session.scalar(
                select(Object).where(Object.name == normalized_name).with_for_update()
            )
            if existing is not None:
                raise ObjectAlreadyExists(normalized_name)
            obj = Object(name=normalized_name, state=ObjectState.ACTIVE)
            self.session.add(obj)
            self.session.flush()
            return obj

    def _lock_object(self, object_id: UUID) -> Object:
        obj = self.session.scalar(
            select(Object).where(Object.object_id == object_id).with_for_update()
        )
        if obj is None:
            raise ObjectNotFound(str(object_id))
        return obj

    def _current_version_number(self, obj: Object) -> Optional[int]:
        if obj.current_version_id is None:
            return None
        current = self.session.scalar(
            select(Version).where(Version.version_id == obj.current_version_id)
        )
        if current is None:
            raise InvalidState(
                f"Object {obj.object_id} points to missing current version."
            )
        return current.version_number

    def create_version(
        self,
        object_id: UUID,
        *,
        size_bytes: int,
        checksum: str,
        expected_current_version: Optional[int] = None,
    ) -> Version:
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
            raise ValueError("size_bytes must be a non-negative integer")
        normalized_checksum = _validate_checksum(checksum)

        with self.session.begin():
            obj = self._lock_object(object_id)
            if obj.state is not ObjectState.ACTIVE:
                raise InvalidState(
                    f"Cannot create a version for object {object_id} in state {obj.state}."
                )

            current_number = self._current_version_number(obj)
            if (
                expected_current_version is not None
                and (
                    not isinstance(expected_current_version, int)
                    or isinstance(expected_current_version, bool)
                    or expected_current_version < 0
                )
            ):
                raise ValueError("expected_current_version must be a non-negative integer or None")

            if (
                expected_current_version is not None
                and current_number != expected_current_version
            ):
                raise VersionConflict(expected_current_version, current_number)

            max_version = self.session.scalar(
                select(func.max(Version.version_number)).where(
                    Version.object_id == object_id
                )
            )
            next_version = int(max_version or 0) + 1

            version = Version(
                object_id=object_id,
                version_number=next_version,
                size_bytes=size_bytes,
                checksum=normalized_checksum,
                state=VersionState.PREPARING,
            )
            self.session.add(version)
            self.session.flush()
            return version

    def commit_version(
        self,
        version_id: UUID,
        *,
        expected_current_version: Optional[int] = None,
    ) -> Version:
        with self.session.begin():
            version = self.session.scalar(
                select(Version).where(Version.version_id == version_id).with_for_update()
            )
            if version is None:
                raise ObjectNotFound(str(version_id))

            obj = self._lock_object(version.object_id)
            current_number = self._current_version_number(obj)

            if expected_current_version is not None and current_number != expected_current_version:
                raise VersionConflict(expected_current_version, current_number)

            if version.state is not VersionState.PREPARING:
                raise InvalidState(
                    f"Version {version.version_id} is {version.state}, not PREPARING."
                )

            if current_number is not None and version.version_number <= current_number:
                raise VersionConflict(version.version_number, current_number)

            version.state = VersionState.COMMITTED
            version.committed_at = datetime.now(timezone.utc)
            obj.current_version_id = version.version_id
            obj.state = ObjectState.ACTIVE
            self.session.flush()
            return version

    def create_replica(self, version_id: UUID, node_id: str) -> Replica:
        if not node_id or not isinstance(node_id, str):
            raise ValueError("node_id must be a non-empty string")

        with self.session.begin():
            version = self.session.scalar(
                select(Version).where(Version.version_id == version_id)
            )
            if version is None:
                raise ObjectNotFound(str(version_id))

            node = self.session.scalar(
                select(StorageNode).where(StorageNode.node_id == node_id)
            )
            if node is None:
                raise ObjectNotFound(node_id)

            existing = self.session.scalar(
                select(Replica)
                .where(
                    Replica.version_id == version_id,
                    Replica.node_id == node_id,
                )
                .with_for_update()
            )
            if existing is not None:
                raise InvalidState(
                    f"Replica already exists for version {version_id} on node {node_id}."
                )

            replica = Replica(
                version_id=version_id,
                node_id=node_id,
                status=ReplicaState.PENDING,
            )
            self.session.add(replica)
            self.session.flush()
            return replica

    def set_replica_state(self, replica_id: UUID, state: ReplicaState) -> Replica:
        if not isinstance(state, ReplicaState):
            raise ValueError("state must be a ReplicaState")

        with self.session.begin():
            replica = self.session.scalar(
                select(Replica).where(Replica.replica_id == replica_id).with_for_update()
            )
            if replica is None:
                raise ObjectNotFound(str(replica_id))

            allowed = {
                ReplicaState.PENDING: {ReplicaState.COPYING, ReplicaState.FAILED},
                ReplicaState.COPYING: {
                    ReplicaState.HEALTHY,
                    ReplicaState.FAILED,
                },
                ReplicaState.HEALTHY: {
                    ReplicaState.STALE,
                    ReplicaState.CORRUPTED,
                    ReplicaState.UNAVAILABLE,
                },
                ReplicaState.STALE: {ReplicaState.REPAIRING},
                ReplicaState.CORRUPTED: {ReplicaState.REPAIRING},
                ReplicaState.UNAVAILABLE: {ReplicaState.REPAIRING},
                ReplicaState.REPAIRING: {ReplicaState.COPYING, ReplicaState.FAILED},
                ReplicaState.FAILED: {ReplicaState.REPAIRING, ReplicaState.COPYING},
            }

            if state not in allowed.get(replica.status, set()):
                raise InvalidState(
                    f"Cannot transition replica from {replica.status} to {state}."
                )

            if state is ReplicaState.HEALTHY:
                raise InvalidState(
                    "Use mark_replica_healthy() after size/checksum verification."
                )

            replica.status = state
            self.session.flush()
            return replica

    def mark_replica_healthy(
        self,
        replica_id: UUID,
        *,
        checksum: str,
        size_bytes: int,
    ) -> Replica:
        actual = _validate_checksum(checksum)
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes < 0:
            raise ValueError("size_bytes must be a non-negative integer")

        with self.session.begin():
            replica = self.session.scalar(
                select(Replica).where(Replica.replica_id == replica_id).with_for_update()
            )
            if replica is None:
                raise ObjectNotFound(str(replica_id))

            version = self.session.scalar(
                select(Version).where(Version.version_id == replica.version_id)
            )
            if version is None:
                raise ObjectNotFound(str(replica.version_id))

            if actual != version.checksum:
                raise ChecksumMismatch(version.checksum, actual)

            if size_bytes != version.size_bytes:
                raise InvalidState(
                    f"Replica size mismatch: expected {version.size_bytes}, got {size_bytes}."
                )

            if replica.status not in {
                ReplicaState.COPYING,
                ReplicaState.REPAIRING,
            }:
                raise InvalidState(
                    f"Replica {replica.replica_id} is {replica.status}; cannot mark HEALTHY."
                )

            replica.status = ReplicaState.HEALTHY
            replica.checksum = actual
            replica.size_bytes = size_bytes
            replica.last_verified_at = datetime.now(timezone.utc)
            self.session.flush()
            return replica

    def register_node(
        self,
        *,
        node_id: Optional[str] = None,
        address: str,
        capacity_bytes: int = 0,
        status: NodeState = NodeState.JOINING,
    ) -> StorageNode:
        if not isinstance(address, str) or not address.strip():
            raise ValueError("address must be a non-empty string")
        if not isinstance(capacity_bytes, int) or isinstance(capacity_bytes, bool) or capacity_bytes < 0:
            raise ValueError("capacity_bytes must be a non-negative integer")
        if not isinstance(status, NodeState):
            raise ValueError("status must be a NodeState")

        normalized_address = address.strip().rstrip("/")
        with self.session.begin():
            existing = self.session.scalar(
                select(StorageNode).where(StorageNode.address == normalized_address).with_for_update()
            )
            if existing is not None:
                if node_id is not None and existing.node_id != node_id:
                    raise ObjectAlreadyExists(normalized_address)
                return existing

            normalized_node_id = node_id or new_uuid().hex
            if not isinstance(normalized_node_id, str) or not normalized_node_id.strip():
                raise ValueError("node_id must be a non-empty string")

            node = StorageNode(
                node_id=normalized_node_id.strip(),
                address=normalized_address,
                capacity_bytes=capacity_bytes,
                used_bytes=0,
                status=status,
            )
            self.session.add(node)
            self.session.flush()
            return node

    def update_node_heartbeat(
        self,
        node_id: str,
        *,
        capacity_bytes: Optional[int] = None,
        used_bytes: Optional[int] = None,
        status: Optional[NodeState] = None,
        heartbeat_at: Optional[datetime] = None,
    ) -> StorageNode:
        now = heartbeat_at or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        with self.session.begin():
            node = self.session.scalar(
                select(StorageNode).where(StorageNode.node_id == node_id).with_for_update()
            )
            if node is None:
                raise ObjectNotFound(node_id)

            if capacity_bytes is not None:
                if not isinstance(capacity_bytes, int) or isinstance(capacity_bytes, bool) or capacity_bytes < 0:
                    raise ValueError("capacity_bytes must be a non-negative integer")
                node.capacity_bytes = capacity_bytes

            if used_bytes is not None:
                if not isinstance(used_bytes, int) or isinstance(used_bytes, bool) or used_bytes < 0:
                    raise ValueError("used_bytes must be a non-negative integer")
                if used_bytes > node.capacity_bytes:
                    raise ValueError("used_bytes cannot exceed capacity_bytes")
                node.used_bytes = used_bytes

            if status is not None:
                if not isinstance(status, NodeState):
                    raise ValueError("status must be a NodeState")
                node.status = status

            current_heartbeat = node.last_heartbeat_at
            if current_heartbeat is None or now >= current_heartbeat:
                node.last_heartbeat_at = now

            self.session.flush()
            return node

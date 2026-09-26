"""Transactional metadata/domain operations for the Vault control plane."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator, Optional
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import OBJECT_STATE_TRANSITIONS, NODE_STATE_TRANSITIONS, NodeState, ObjectState, ReplicaState, VersionState
from common.errors import (
    ChecksumMismatch,
    InvalidState,
    ObjectAlreadyExists,
    ObjectNotFound,
    VersionConflict,
)
from common.ids import new_uuid
from common.validation import normalize_object_name

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

    @contextmanager
    def _transaction(self) -> Iterator[Session]:
        """Run an operation atomically while remaining composable with caller transactions."""
        if self.session.in_transaction():
            with self.session.begin_nested():
                yield self.session
        else:
            with self.session.begin():
                yield self.session

    def get_object(self, name: str) -> Optional[Object]:
        normalized_name = normalize_object_name(name)
        with self._transaction():
            return self.session.scalar(select(Object).where(Object.name == normalized_name))

    def get_object_or_raise(self, name: str) -> Object:
        obj = self.get_object(name)
        if obj is None:
            raise ObjectNotFound(name.strip() if isinstance(name, str) else str(name))
        return obj

    def create_object(self, name: str) -> Object:
        normalized_name = normalize_object_name(name)
        with self._transaction():
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

    @staticmethod
    def _validate_expected_version(value: Optional[int]) -> None:
        if value is None:
            return
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(
                "expected_current_version must be a non-negative integer or None"
            )

    @staticmethod
    def _expected_matches(
        expected_current_version: Optional[int],
        current_number: Optional[int],
    ) -> bool:
        if expected_current_version is None:
            return True
        # A brand-new object has logical current version 0 for conditional writes.
        actual = 0 if current_number is None else current_number
        return actual == expected_current_version

    def create_version(
        self,
        object_id: UUID,
        *,
        size_bytes: int,
        checksum: str,
        expected_current_version: Optional[int] = None,
    ) -> Version:
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise ValueError("size_bytes must be a non-negative integer")

        normalized_checksum = _validate_checksum(checksum)
        self._validate_expected_version(expected_current_version)

        with self._transaction():
            obj = self._lock_object(object_id)
            if obj.state is not ObjectState.ACTIVE:
                raise InvalidState(
                    f"Cannot create a version for object {object_id} in state {obj.state}."
                )

            current_number = self._current_version_number(obj)
            if not self._expected_matches(
                expected_current_version, current_number
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
        self._validate_expected_version(expected_current_version)

        with self._transaction():
            version = self.session.scalar(
                select(Version).where(Version.version_id == version_id).with_for_update()
            )
            if version is None:
                raise ObjectNotFound(str(version_id))

            obj = self._lock_object(version.object_id)
            current_number = self._current_version_number(obj)

            if not self._expected_matches(
                expected_current_version, current_number
            ):
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

    def transition_object_state(self, object_id: UUID, state: ObjectState) -> Object:
        """Apply one canonical object lifecycle transition transactionally."""
        if not isinstance(object_id, UUID):
            raise ValueError("object_id must be a UUID")
        if not isinstance(state, ObjectState):
            raise ValueError("state must be an ObjectState")
        with self._transaction():
            obj = self.session.scalar(
                select(Object).where(Object.object_id == object_id).with_for_update()
            )
            if obj is None:
                raise ObjectNotFound(str(object_id))
            if state is obj.state:
                return obj
            if state not in OBJECT_STATE_TRANSITIONS.get(obj.state, frozenset()):
                raise InvalidState(
                    f"Cannot transition object {object_id} from {obj.state} to {state}."
                )
            obj.state = state
            self.session.flush()
            return obj

    def fail_version(self, version_id: UUID) -> Version:
        """Mark a provisional version FAILED without exposing it as current."""
        if not isinstance(version_id, UUID):
            raise ValueError("version_id must be a UUID")
        with self._transaction():
            version = self.session.scalar(
                select(Version).where(Version.version_id == version_id).with_for_update()
            )
            if version is None:
                raise ObjectNotFound(str(version_id))
            if version.state is VersionState.COMMITTED:
                raise InvalidState(f"Version {version_id} is already COMMITTED.")
            version.state = VersionState.FAILED
            self.session.flush()
            return version

    def transition_node_state(self, node_id: str, state: NodeState) -> StorageNode:
        """Apply one canonical node lifecycle transition transactionally."""
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if not isinstance(state, NodeState):
            raise ValueError("state must be a NodeState")
        normalized_node_id = node_id.strip()
        with self._transaction():
            node = self.session.scalar(
                select(StorageNode)
                .where(StorageNode.node_id == normalized_node_id)
                .with_for_update()
            )
            if node is None:
                raise ObjectNotFound(normalized_node_id)
            if state is node.status:
                return node
            if state not in NODE_STATE_TRANSITIONS.get(node.status, frozenset()):
                raise InvalidState(
                    f"Cannot transition node {normalized_node_id} from "
                    f"{node.status} to {state}."
                )
            node.status = state
            self.session.flush()
            return node

    def create_replica(self, version_id: UUID, node_id: str) -> Replica:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        normalized_node_id = node_id.strip()

        with self._transaction():
            version = self.session.scalar(
                select(Version).where(Version.version_id == version_id)
            )
            if version is None:
                raise ObjectNotFound(str(version_id))

            node = self.session.scalar(
                select(StorageNode).where(StorageNode.node_id == normalized_node_id)
            )
            if node is None:
                raise ObjectNotFound(normalized_node_id)

            existing = self.session.scalar(
                select(Replica)
                .where(
                    Replica.version_id == version_id,
                    Replica.node_id == normalized_node_id,
                )
                .with_for_update()
            )
            if existing is not None:
                raise InvalidState(
                    f"Replica already exists for version {version_id} on node {normalized_node_id}."
                )

            replica = Replica(
                version_id=version_id,
                node_id=normalized_node_id,
                status=ReplicaState.PENDING,
            )
            self.session.add(replica)
            self.session.flush()
            return replica

    def set_replica_state(self, replica_id: UUID, state: ReplicaState) -> Replica:
        if not isinstance(state, ReplicaState):
            raise ValueError("state must be a ReplicaState")

        with self._transaction():
            replica = self.session.scalar(
                select(Replica).where(Replica.replica_id == replica_id).with_for_update()
            )
            if replica is None:
                raise ObjectNotFound(str(replica_id))

            allowed = {
                ReplicaState.PENDING: {ReplicaState.REPAIRING, ReplicaState.COPYING, ReplicaState.FAILED},
                ReplicaState.COPYING: {ReplicaState.FAILED},
                ReplicaState.HEALTHY: {
                    ReplicaState.STALE,
                    ReplicaState.CORRUPTED,
                    ReplicaState.UNAVAILABLE,
                },
                ReplicaState.STALE: {ReplicaState.REPAIRING},
                ReplicaState.CORRUPTED: {ReplicaState.REPAIRING},
                ReplicaState.UNAVAILABLE: {ReplicaState.REPAIRING},
                ReplicaState.REPAIRING: {ReplicaState.COPYING, ReplicaState.FAILED},
                # FAILED is terminal for that replica operation. A future repair
                # creates/uses a new job rather than inventing an extra transition.
                ReplicaState.FAILED: set(),
            }

            if replica.status is state:
                return replica

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

    def record_replica_verification(
        self,
        replica_id: UUID,
        *,
        checksum: str,
        size_bytes: int,
    ) -> Replica:
        """Persist a successful integrity verification for an already-healthy replica."""
        actual = _validate_checksum(checksum)
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise ValueError("size_bytes must be a non-negative integer")

        with self._transaction():
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

            if replica.status is not ReplicaState.HEALTHY:
                raise InvalidState(
                    f"Replica {replica.replica_id} is {replica.status}; "
                    "integrity verification requires HEALTHY state."
                )
            if actual != version.checksum:
                raise ChecksumMismatch(version.checksum, actual)
            if size_bytes != version.size_bytes:
                raise InvalidState(
                    f"Replica size mismatch: expected {version.size_bytes}, got {size_bytes}."
                )

            replica.checksum = actual
            replica.size_bytes = size_bytes
            replica.last_verified_at = datetime.now(timezone.utc)
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
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise ValueError("size_bytes must be a non-negative integer")

        with self._transaction():
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

    def remove_replica(self, replica_id: UUID) -> Replica:
        """Remove metadata only after storage deletion is already safe."""
        if not isinstance(replica_id, UUID):
            raise ValueError("replica_id must be a UUID")
        with self._transaction():
            replica = self.session.scalar(
                select(Replica).where(Replica.replica_id == replica_id).with_for_update()
            )
            if replica is None:
                raise ObjectNotFound(str(replica_id))
            if replica.status not in {
                ReplicaState.HEALTHY,
                ReplicaState.CORRUPTED,
                ReplicaState.STALE,
                ReplicaState.UNAVAILABLE,
            }:
                raise InvalidState(
                    f"Replica {replica.replica_id} cannot be removed from state {replica.status}."
                )
            self.session.delete(replica)
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
        if (
            not isinstance(capacity_bytes, int)
            or isinstance(capacity_bytes, bool)
            or capacity_bytes < 0
        ):
            raise ValueError("capacity_bytes must be a non-negative integer")
        if not isinstance(status, NodeState):
            raise ValueError("status must be a NodeState")

        normalized_address = address.strip().rstrip("/")
        with self._transaction():
            existing = self.session.scalar(
                select(StorageNode)
                .where(StorageNode.address == normalized_address)
                .with_for_update()
            )
            if existing is not None:
                if node_id is not None and existing.node_id != node_id.strip():
                    raise ObjectAlreadyExists(normalized_address)
                return existing

            normalized_node_id = node_id.strip() if node_id is not None else new_uuid().hex
            if not normalized_node_id:
                raise ValueError("node_id must be a non-empty string")

            id_conflict = self.session.scalar(
                select(StorageNode)
                .where(StorageNode.node_id == normalized_node_id)
                .with_for_update()
            )
            if id_conflict is not None:
                raise ObjectAlreadyExists(normalized_node_id)

            node = StorageNode(
                node_id=normalized_node_id,
                address=normalized_address,
                capacity_bytes=capacity_bytes,
                used_bytes=0,
                status=status,
            )
            self.session.add(node)
            self.session.flush()
            return node

    def process_node_heartbeat(
        self,
        node_id: str,
        *,
        capacity_bytes: Optional[int],
        used_bytes: Optional[int],
        heartbeat_at: datetime,
    ) -> tuple[StorageNode, bool]:
        """Record a heartbeat only if it is strictly newer than the stored one."""
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        if capacity_bytes is not None and (
            not isinstance(capacity_bytes, int) or isinstance(capacity_bytes, bool) or capacity_bytes < 0
        ):
            raise ValueError("capacity_bytes must be a non-negative integer")
        if used_bytes is not None and (
            not isinstance(used_bytes, int) or isinstance(used_bytes, bool) or used_bytes < 0
        ):
            raise ValueError("used_bytes must be a non-negative integer")
        if not isinstance(heartbeat_at, datetime):
            raise ValueError("heartbeat_at must be a datetime")
        timestamp = heartbeat_at
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        normalized_node_id = node_id.strip()
        with self._transaction():
            node = self.session.scalar(
                select(StorageNode)
                .where(StorageNode.node_id == normalized_node_id)
                .with_for_update()
            )
            if node is None:
                raise ObjectNotFound(normalized_node_id)
            if node.status is NodeState.REMOVED:
                raise InvalidState(
                    f"Removed node {normalized_node_id} must be registered again before heartbeat."
                )
            current = node.last_heartbeat_at
            if current is not None:
                if current.tzinfo is None:
                    current = current.replace(tzinfo=timezone.utc)
                else:
                    current = current.astimezone(timezone.utc)
                if timestamp <= current:
                    return node, False
            next_capacity = node.capacity_bytes if capacity_bytes is None else capacity_bytes
            next_used = node.used_bytes if used_bytes is None else used_bytes
            if next_used > next_capacity:
                raise ValueError("used_bytes cannot exceed capacity_bytes")
            node.capacity_bytes = next_capacity
            node.used_bytes = next_used
            node.last_heartbeat_at = timestamp
            self.session.flush()
            return node, True

    def update_node_heartbeat(
        self,
        node_id: str,
        *,
        capacity_bytes: Optional[int] = None,
        used_bytes: Optional[int] = None,
        status: Optional[NodeState] = None,
        heartbeat_at: Optional[datetime] = None,
    ) -> StorageNode:
        """Backward-compatible heartbeat update with monotonic timestamp handling."""
        timestamp = heartbeat_at or datetime.now(timezone.utc)
        node, accepted = self.process_node_heartbeat(
            node_id,
            capacity_bytes=capacity_bytes,
            used_bytes=used_bytes,
            heartbeat_at=timestamp,
        )
        if status is not None:
            if not isinstance(status, NodeState):
                raise ValueError("status must be a NodeState")
            if accepted and node.status is not status:
                node = self.transition_node_state(node.node_id, status)
        return node

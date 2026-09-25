"""Background-safe integrity verification and corruption-triggered repair."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import ObjectNotFound
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode, Version
from repair import RepairManager
from replication.node_client import StorageNodeClient


@dataclass(frozen=True, slots=True)
class IntegrityResult:
    replica_id: UUID
    version_id: UUID
    node_id: str
    checked: bool
    corrupted: bool
    repair_id: UUID | None = None
    checksum: str | None = None
    size_bytes: int | None = None


class IntegrityManager:
    """Verify stored replicas against metadata without touching node filesystems."""

    def __init__(
        self,
        session: Session,
        *,
        client_factory=StorageNodeClient,
        repair_manager_factory=RepairManager,
        replication_factor: int = 3,
    ) -> None:
        replication_factor = get_settings().replication_factor if replication_factor is None else replication_factor
        if (
            not isinstance(replication_factor, int)
            or isinstance(replication_factor, bool)
            or replication_factor < 1
        ):
            raise ValueError("replication_factor must be a positive integer")
        self.session = session
        self.metadata = MetadataManager(session)
        self.client_factory = client_factory
        self.repair_manager_factory = repair_manager_factory
        self.replication_factor = replication_factor

    def _replica(self, replica_id: UUID) -> Replica:
        replica = self.session.scalar(
            select(Replica).where(Replica.replica_id == replica_id)
        )
        if replica is None:
            raise ObjectNotFound(str(replica_id))
        return replica

    def _version(self, version_id: UUID) -> Version:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        return version

    def _node(self, node_id: str) -> StorageNode:
        node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == node_id)
        )
        if node is None:
            raise ObjectNotFound(node_id)
        return node

    async def verify_replica(self, replica_id: UUID) -> IntegrityResult:
        """Verify one HEALTHY committed replica and enqueue repair on corruption."""
        replica = self._replica(replica_id)
        version = self._version(replica.version_id)
        node = self._node(replica.node_id)

        if version.state is not VersionState.COMMITTED:
            return IntegrityResult(
                replica.replica_id,
                replica.version_id,
                replica.node_id,
                checked=False,
                corrupted=False,
            )

        # Temporary unreachability is not proof of corruption. Health processing
        # owns node-state transitions; integrity work only inspects healthy nodes.
        if node.status is not NodeState.HEALTHY or replica.status is not ReplicaState.HEALTHY:
            return IntegrityResult(
                replica.replica_id,
                replica.version_id,
                replica.node_id,
                checked=False,
                corrupted=False,
            )

        client = self.client_factory(node.address)
        try:
            measured = await client.verify_object(
                str(version.object_id),
                str(version.version_id),
            )

            expected_checksum = (replica.checksum or version.checksum).lower()
            expected_size = replica.size_bytes if replica.size_bytes is not None else version.size_bytes
            actual_checksum = measured.checksum.lower()
            actual_size = measured.size_bytes
            valid = bool(measured.verified and measured.valid)
            matches = (
                valid
                and actual_checksum == expected_checksum
                and actual_checksum == version.checksum.lower()
                and actual_size == expected_size
                and actual_size == version.size_bytes
            )

            if not matches:
                # Mark corruption only after the storage node was successfully
                # contacted and returned a verifiable measurement. A timeout,
                # connection error, or health issue must not be called corruption.
                self.metadata.set_replica_state(
                    replica.replica_id,
                    ReplicaState.CORRUPTED,
                )
                repair_manager = self.repair_manager_factory(self.session)
                job = repair_manager.schedule_for_version(
                    version.version_id,
                    replication_factor=self.replication_factor,
                    reason="integrity-mismatch",
                    preferred_replica=replica,
                )
                return IntegrityResult(
                    replica.replica_id,
                    replica.version_id,
                    replica.node_id,
                    checked=True,
                    corrupted=True,
                    repair_id=None if job is None else job.repair_id,
                    checksum=actual_checksum,
                    size_bytes=actual_size,
                )

            self.metadata.record_replica_verification(
                replica.replica_id,
                checksum=actual_checksum,
                size_bytes=actual_size,
            )
            return IntegrityResult(
                replica.replica_id,
                replica.version_id,
                replica.node_id,
                checked=True,
                corrupted=False,
                checksum=actual_checksum,
                size_bytes=actual_size,
            )
        finally:
            with suppress(Exception):
                await client.aclose()

    async def scan_node(self, node_id: str) -> list[IntegrityResult]:
        """Verify all healthy committed replicas currently assigned to one healthy node."""
        node = self._node(node_id)
        if node.status is not NodeState.HEALTHY:
            return []

        replica_ids = list(
            self.session.scalars(
                select(Replica.replica_id)
                .join(Version, Version.version_id == Replica.version_id)
                .where(
                    Replica.node_id == node_id,
                    Replica.status == ReplicaState.HEALTHY,
                    Version.state == VersionState.COMMITTED,
                )
                .order_by(Replica.replica_id.asc())
            ).all()
        )
        results: list[IntegrityResult] = []
        for replica_id in replica_ids:
            results.append(await self.verify_replica(replica_id))
        return results

    async def scan_version(self, version_id: UUID) -> list[IntegrityResult]:
        """Verify every healthy replica of one committed version."""
        version = self._version(version_id)
        if version.state is not VersionState.COMMITTED:
            return []

        replica_ids = list(
            self.session.scalars(
                select(Replica.replica_id)
                .where(
                    Replica.version_id == version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
                .order_by(Replica.node_id.asc())
            ).all()
        )
        results: list[IntegrityResult] = []
        for replica_id in replica_ids:
            results.append(await self.verify_replica(replica_id))
        return results

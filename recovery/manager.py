"""Network-partition recovery and replica reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import InvalidState, ObjectNotFound, VaultError
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, StorageNode, Version
from repair import RepairManager
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageNodeIntegrityError,
    StorageObjectNotFoundError,
)


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    node_id: str
    checked_replicas: int
    healthy_replicas: int
    missing_replica_ids: tuple[UUID, ...] = ()
    corrupted_replica_ids: tuple[UUID, ...] = ()
    repair_ids: tuple[UUID, ...] = ()
    unresolved_replica_ids: tuple[UUID, ...] = ()
    complete: bool = True
    errors: tuple[str, ...] = ()


class PartitionRecoveryManager:
    """Reconcile a recovered node without treating a partition as data loss."""

    def __init__(
        self,
        session: Session,
        *,
        client_factory=StorageNodeClient,
        repair_manager_factory=RepairManager,
        replication_factor: int = 3,
    ) -> None:
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

    def _node(self, node_id: str) -> StorageNode:
        node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == node_id)
        )
        if node is None:
            raise ObjectNotFound(node_id)
        return node

    def _known_replicas(self, node_id: str) -> list[Replica]:
        return list(
            self.session.scalars(
                select(Replica)
                .join(Version, Version.version_id == Replica.version_id)
                .where(
                    Replica.node_id == node_id,
                    Version.state == VersionState.COMMITTED,
                    Replica.status != ReplicaState.FAILED,
                )
                .order_by(Replica.replica_id.asc())
            ).all()
        )

    def _version(self, version_id: UUID) -> Version:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        return version

    def _mark_verified_replica(
        self,
        replica: Replica,
        *,
        checksum: str,
        size_bytes: int,
    ) -> None:
        if replica.status is ReplicaState.HEALTHY:
            self.metadata.record_replica_verification(
                replica.replica_id,
                checksum=checksum,
                size_bytes=size_bytes,
            )
            return
        if replica.status is ReplicaState.FAILED:
            return
        if replica.status is ReplicaState.PENDING:
            self.metadata.set_replica_state(
                replica.replica_id,
                ReplicaState.REPAIRING,
            )
        elif replica.status in {
            ReplicaState.CORRUPTED,
            ReplicaState.STALE,
            ReplicaState.UNAVAILABLE,
        }:
            self.metadata.set_replica_state(
                replica.replica_id,
                ReplicaState.REPAIRING,
            )
        elif replica.status is not ReplicaState.REPAIRING:
            raise InvalidState(
                f"Replica {replica.replica_id} cannot be reconciled from state {replica.status}."
            )
        self.metadata.mark_replica_healthy(
            replica.replica_id,
            checksum=checksum,
            size_bytes=size_bytes,
        )

    def _schedule_repair(
        self,
        version_id: UUID,
        replica: Replica,
        *,
        reason: str,
    ) -> tuple[UUID | None, str | None]:
        repair_manager = self.repair_manager_factory(self.session)
        try:
            job = repair_manager.schedule_for_version(
                version_id,
                replication_factor=self.replication_factor,
                reason=reason,
                preferred_replica=replica,
            )
        except VaultError as exc:
            return None, exc.message
        if job is None:
            return None, None
        return job.repair_id, None

    async def reconcile_node(self, node_id: str) -> RecoveryResult:
        """Verify known committed replicas on a node in RECOVERING state."""
        node = self._node(node_id)
        if node.status is not NodeState.RECOVERING:
            raise InvalidState(
                f"Node {node_id} must be RECOVERING before reconciliation; got {node.status}."
            )

        replicas = self._known_replicas(node_id)
        checked = 0
        healthy = 0
        missing: list[UUID] = []
        corrupted: list[UUID] = []
        repair_ids: list[UUID] = []
        unresolved: list[UUID] = []
        errors: list[str] = []
        complete = True

        for replica in replicas:
            version = self._version(replica.version_id)
            client = self.client_factory(node.address)
            try:
                measured = await client.verify_object(
                    str(version.object_id),
                    str(version.version_id),
                )
                checked += 1
                expected_checksum = version.checksum.lower()
                expected_size = version.size_bytes
                if (
                    not measured.verified
                    or not measured.valid
                    or measured.checksum.lower() != expected_checksum
                    or measured.size_bytes != expected_size
                ):
                    corrupted.append(replica.replica_id)
                    if replica.status is ReplicaState.HEALTHY:
                        self.metadata.set_replica_state(
                            replica.replica_id,
                            ReplicaState.CORRUPTED,
                        )
                    repair_id, repair_error = self._schedule_repair(
                        version.version_id,
                        replica,
                        reason="partition-recovery-integrity-mismatch",
                    )
                    if repair_id is not None:
                        repair_ids.append(repair_id)
                    if repair_error is not None:
                        unresolved.append(replica.replica_id)
                        errors.append(f"{replica.replica_id}: {repair_error}")
                    continue

                self._mark_verified_replica(
                    replica,
                    checksum=measured.checksum,
                    size_bytes=measured.size_bytes,
                )
                healthy += 1
            except StorageObjectNotFoundError:
                checked += 1
                missing.append(replica.replica_id)
                if replica.status is ReplicaState.HEALTHY:
                    self.metadata.set_replica_state(
                        replica.replica_id,
                        ReplicaState.UNAVAILABLE,
                    )
                repair_id, repair_error = self._schedule_repair(
                    version.version_id,
                    replica,
                    reason="partition-recovery-missing",
                )
                if repair_id is not None:
                    repair_ids.append(repair_id)
                if repair_error is not None:
                    unresolved.append(replica.replica_id)
                    errors.append(f"{replica.replica_id}: {repair_error}")
            except StorageNodeIntegrityError as exc:
                checked += 1
                corrupted.append(replica.replica_id)
                if replica.status is ReplicaState.HEALTHY:
                    self.metadata.set_replica_state(
                        replica.replica_id,
                        ReplicaState.CORRUPTED,
                    )
                repair_id, repair_error = self._schedule_repair(
                    version.version_id,
                    replica,
                    reason="partition-recovery-integrity-mismatch",
                )
                if repair_id is not None:
                    repair_ids.append(repair_id)
                if repair_error is not None:
                    unresolved.append(replica.replica_id)
                    errors.append(f"{replica.replica_id}: {repair_error}")
                errors.append(f"{replica.replica_id}: {exc}")
            except StorageNodeClientError as exc:
                unresolved.append(replica.replica_id)
                complete = False
                errors.append(f"{replica.replica_id}: {exc}")
            finally:
                await client.aclose()

        return RecoveryResult(
            node_id=node_id,
            checked_replicas=checked,
            healthy_replicas=healthy,
            missing_replica_ids=tuple(missing),
            corrupted_replica_ids=tuple(corrupted),
            repair_ids=tuple(repair_ids),
            unresolved_replica_ids=tuple(unresolved),
            complete=complete and not unresolved,
            errors=tuple(errors),
        )

    async def reconcile_and_mark_healthy(self, node_id: str) -> RecoveryResult:
        """Reconcile known data, then return the node to HEALTHY when complete."""
        result = await self.reconcile_node(node_id)
        if result.complete:
            node = self._node(node_id)
            if node.status is NodeState.RECOVERING:
                self.metadata.transition_node_state(node_id, NodeState.HEALTHY)
        return result

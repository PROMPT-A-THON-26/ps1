"""Safe replica migration and storage-node drain orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import ErrorCode, JobStatus, NodeState, ReplicaState, VersionState
from common.settings import settings
from common.errors import InvalidState, ObjectNotFound, VaultError
from metadata.manager import MetadataManager
from metadata.models import RebalanceJob, Replica, StorageNode, Version
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageNodeIntegrityError,
)


@dataclass(frozen=True, slots=True)
class RebalanceResult:
    rebalance_id: UUID
    version_id: UUID
    source_node_id: str
    target_node_id: str
    status: JobStatus
    attempts: int
    source_removed: bool


class RebalanceManager:
    """Migrate replicas copy-before-delete and persist progress for retry."""

    def __init__(
        self,
        session: Session,
        *,
        client_factory=StorageNodeClient,
        replication_factor: int | None = None,
        max_attempts: int | None = None,
    ) -> None:
        replication_factor = settings.replication_factor if replication_factor is None else replication_factor
        max_attempts = settings.max_attempts if max_attempts is None else max_attempts
        if (
            not isinstance(replication_factor, int)
            or isinstance(replication_factor, bool)
            or replication_factor < 1
        ):
            raise ValueError("replication_factor must be a positive integer")
        if (
            not isinstance(max_attempts, int)
            or isinstance(max_attempts, bool)
            or max_attempts < 1
        ):
            raise ValueError("max_attempts must be a positive integer")
        self.session = session
        self.metadata = MetadataManager(session)
        self.client_factory = client_factory
        self.replication_factor = settings.replication_factor if replication_factor is None else replication_factor
        self.max_attempts = settings.max_attempts if max_attempts is None else max_attempts

    def _version(self, version_id: UUID) -> Version:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        if version.state is not VersionState.COMMITTED:
            raise InvalidState(
                f"Version {version_id} must be COMMITTED before rebalancing."
            )
        return version

    def _node(self, node_id: str) -> StorageNode:
        node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == node_id)
        )
        if node is None:
            raise ObjectNotFound(node_id)
        return node

    def _active_job(self, version_id: UUID, target_node_id: str) -> RebalanceJob | None:
        return self.session.scalar(
            select(RebalanceJob)
            .where(
                RebalanceJob.version_id == version_id,
                RebalanceJob.target_node_id == target_node_id,
                RebalanceJob.status.in_((JobStatus.PENDING, JobStatus.RUNNING)),
            )
            .order_by(RebalanceJob.created_at.asc())
        )

    def _eligible_target(
        self,
        version: Version,
        *,
        excluded_node_ids: set[str],
    ) -> StorageNode:
        replica_exists = select(Replica.replica_id).where(
            Replica.version_id == version.version_id,
            Replica.node_id == StorageNode.node_id,
        ).exists()
        statement = (
            select(StorageNode)
            .where(
                StorageNode.status == NodeState.HEALTHY,
                StorageNode.capacity_bytes - StorageNode.used_bytes >= version.size_bytes,
                ~replica_exists,
            )
            .order_by(
                (StorageNode.capacity_bytes - StorageNode.used_bytes).desc(),
                StorageNode.node_id.asc(),
            )
            .limit(1)
        )
        if excluded_node_ids:
            statement = statement.where(
                StorageNode.node_id.not_in(excluded_node_ids)
            )
        candidate = self.session.scalar(statement)
        if candidate is None:
            raise VaultError(
                code=ErrorCode.INSUFFICIENT_REPLICAS,
                message=f"No healthy target node can hold version {version.version_id}.",
                status_code=503,
            )
        return candidate

    def create_job(
        self,
        version_id: UUID,
        *,
        source_node_id: str,
        target_node_id: str,
    ) -> RebalanceJob:
        version = self._version(version_id)
        if source_node_id == target_node_id:
            raise ValueError("rebalance source and target must be different nodes")

        self._node(source_node_id)
        target_node = self._node(target_node_id)

        source_replica = self.session.scalar(
            select(Replica).where(
                Replica.version_id == version_id,
                Replica.node_id == source_node_id,
            )
        )
        if source_replica is None:
            raise ObjectNotFound(
                f"Replica for version {version_id} on {source_node_id}"
            )
        if source_replica.status is not ReplicaState.HEALTHY:
            raise InvalidState(
                f"Rebalance source replica must be HEALTHY; got {source_replica.status}."
            )
        if target_node.status is not NodeState.HEALTHY:
            raise VaultError(
                code=ErrorCode.NODE_UNAVAILABLE,
                message=f"Rebalance target node {target_node_id} is not healthy.",
                status_code=503,
            )
        if target_node.free_bytes < version.size_bytes:
            raise VaultError(
                code=ErrorCode.STORAGE_FULL,
                message=f"Rebalance target node {target_node_id} lacks capacity.",
                status_code=507,
            )

        existing_replica = self.session.scalar(
            select(Replica).where(
                Replica.version_id == version_id,
                Replica.node_id == target_node_id,
            )
        )
        if existing_replica is not None:
            raise InvalidState(
                f"Target node {target_node_id} already has replica {existing_replica.replica_id}."
            )

        active = self._active_job(version_id, target_node_id)
        if active is not None:
            return active

        job = RebalanceJob(
            version_id=version_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            status=JobStatus.PENDING,
            attempts=0,
        )
        self.session.add(job)
        self.session.commit()
        return job

    def _target_replica(self, job: RebalanceJob) -> Replica:
        replica = self.session.scalar(
            select(Replica)
            .where(
                Replica.version_id == job.version_id,
                Replica.node_id == job.target_node_id,
            )
            .with_for_update()
        )
        if replica is None:
            replica = self.metadata.create_replica(
                job.version_id,
                job.target_node_id,
            )

        if replica.status is ReplicaState.HEALTHY:
            return replica

        if replica.status is ReplicaState.PENDING:
            self.metadata.set_replica_state(
                replica.replica_id,
                ReplicaState.COPYING,
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
            self.metadata.set_replica_state(
                replica.replica_id,
                ReplicaState.COPYING,
            )
        elif replica.status is not ReplicaState.COPYING:
            raise InvalidState(
                f"Rebalance target replica cannot enter COPYING from {replica.status}."
            )
        return replica

    def _healthy_replica_count(self, version_id: UUID) -> int:
        return int(
            self.session.scalar(
                select(func.count(Replica.replica_id)).where(
                    Replica.version_id == version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
            )
            or 0
        )

    def _matches_metadata(self, measured, version: Version) -> bool:
        return (
            measured.verified
            and measured.valid
            and measured.checksum.lower() == version.checksum.lower()
            and measured.size_bytes == version.size_bytes
        )

    def _mark_job(
        self,
        job: RebalanceJob,
        status: JobStatus,
        *,
        error: str | None = None,
    ) -> None:
        job.status = status
        job.last_error = error
        job.updated_at = datetime.now(timezone.utc)
        self.session.commit()

    async def run_job(self, rebalance_id: UUID) -> RebalanceResult:
        job = self.session.scalar(
            select(RebalanceJob).where(RebalanceJob.rebalance_id == rebalance_id)
        )
        if job is None:
            raise ObjectNotFound(str(rebalance_id))
        if job.status is JobStatus.SUCCEEDED:
            return RebalanceResult(
                job.rebalance_id,
                job.version_id,
                job.source_node_id,
                job.target_node_id,
                job.status,
                job.attempts,
                source_removed=True,
            )
        if job.status is JobStatus.FAILED and job.attempts >= self.max_attempts:
            return RebalanceResult(
                job.rebalance_id,
                job.version_id,
                job.source_node_id,
                job.target_node_id,
                job.status,
                job.attempts,
                source_removed=False,
            )

        version = self._version(job.version_id)
        source_node = self._node(job.source_node_id)
        target_node = self._node(job.target_node_id)
        target_replica = self.session.scalar(
            select(Replica).where(
                Replica.version_id == job.version_id,
                Replica.node_id == job.target_node_id,
            )
        )

        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.last_error = None
        self.session.commit()

        source_client = self.client_factory(source_node.address)
        target_client = self.client_factory(target_node.address)
        source_removed = False

        try:
            # A target that was successfully copied in a previous attempt only
            # needs verification before the old source can be removed.
            if target_replica is not None and target_replica.status is ReplicaState.HEALTHY:
                target_verified = await target_client.verify_object(
                    str(version.object_id),
                    str(version.version_id),
                )
                if not self._matches_metadata(target_verified, version):
                    self.metadata.set_replica_state(
                        target_replica.replica_id,
                        ReplicaState.CORRUPTED,
                    )
                    target_replica = None

            if target_replica is None or target_replica.status is not ReplicaState.HEALTHY:
                source_replica = self.session.scalar(
                    select(Replica).where(
                        Replica.version_id == job.version_id,
                        Replica.node_id == job.source_node_id,
                        Replica.status == ReplicaState.HEALTHY,
                    )
                )
                if source_replica is None:
                    raise InvalidState(
                        f"Verified healthy source {job.source_node_id} is no longer available."
                    )

                source_verified = await source_client.verify_object(
                    str(version.object_id),
                    str(version.version_id),
                )
                if not self._matches_metadata(source_verified, version):
                    raise StorageNodeIntegrityError(
                        "Rebalance source verification does not match metadata."
                    )

                target_replica = self._target_replica(job)
                await target_client.delete_object(
                    str(version.object_id),
                    str(version.version_id),
                )

                async with source_client.stream_object(
                    str(version.object_id),
                    str(version.version_id),
                ) as response:
                    await target_client.put_object(
                        str(version.object_id),
                        str(version.version_id),
                        response.aiter_bytes(),
                    )

                target_verified = await target_client.verify_object(
                    str(version.object_id),
                    str(version.version_id),
                )
                if not self._matches_metadata(target_verified, version):
                    raise StorageNodeIntegrityError(
                        "Rebalance target verification does not match metadata."
                    )

                self.metadata.mark_replica_healthy(
                    target_replica.replica_id,
                    checksum=target_verified.checksum,
                    size_bytes=target_verified.size_bytes,
                )

            # Copy and target verification are complete at this point. Only now
            # is it legal to remove the old source, and only if RF remains safe.
            if self._healthy_replica_count(job.version_id) > self.replication_factor:
                await source_client.delete_object(
                    str(version.object_id),
                    str(version.version_id),
                )
                source_replica = self.session.scalar(
                    select(Replica).where(
                        Replica.version_id == job.version_id,
                        Replica.node_id == job.source_node_id,
                    )
                )
                if source_replica is not None:
                    self.metadata.remove_replica(source_replica.replica_id)
                source_removed = True
            else:
                raise InvalidState(
                    "Rebalance target is healthy but removing the source would violate replication factor."
                )

            self._mark_job(job, JobStatus.SUCCEEDED)
            return RebalanceResult(
                job.rebalance_id,
                job.version_id,
                job.source_node_id,
                job.target_node_id,
                job.status,
                job.attempts,
                source_removed=source_removed,
            )
        except (StorageNodeClientError, VaultError, InvalidState) as exc:
            self._mark_job(
                job,
                JobStatus.FAILED if job.attempts >= self.max_attempts else JobStatus.PENDING,
                error=str(exc),
            )
            raise
        finally:
            await source_client.aclose()
            await target_client.aclose()

    async def migrate_replica(
        self,
        version_id: UUID,
        *,
        source_node_id: str,
        target_node_id: str,
    ) -> RebalanceResult:
        job = self.create_job(
            version_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
        )
        return await self.run_job(job.rebalance_id)

    def _source_for_version(
        self,
        version_id: UUID,
        *,
        excluded_node_id: str,
    ) -> Replica:
        source = self.session.scalar(
            select(Replica)
            .join(StorageNode, StorageNode.node_id == Replica.node_id)
            .where(
                Replica.version_id == version_id,
                Replica.node_id != excluded_node_id,
                Replica.status == ReplicaState.HEALTHY,
                StorageNode.status == NodeState.HEALTHY,
                Replica.checksum.is_not(None),
            )
            .order_by(
                Replica.last_verified_at.desc(),
                Replica.node_id.asc(),
            )
        )
        if source is None:
            raise VaultError(
                code="NODE_UNAVAILABLE",
                message=f"No healthy source remains for version {version_id}.",
                status_code=503,
            )
        return source

    async def drain_node(self, node_id: str) -> list[RebalanceResult]:
        """Drain a node and remove it only after all replica metadata is gone."""
        node = self._node(node_id)
        if node.status is NodeState.HEALTHY:
            self.metadata.transition_node_state(node_id, NodeState.DRAINING)
            node = self._node(node_id)
        if node.status is not NodeState.DRAINING:
            raise InvalidState(
                f"Node {node_id} must be DRAINING before migration; got {node.status}."
            )

        results: list[RebalanceResult] = []
        replica_ids = list(
            self.session.scalars(
                select(Replica.replica_id)
                .join(Version, Version.version_id == Replica.version_id)
                .where(
                    Replica.node_id == node_id,
                    Version.state == VersionState.COMMITTED,
                )
                .order_by(Replica.replica_id.asc())
            ).all()
        )

        for replica_id in replica_ids:
            replica = self.session.get(Replica, replica_id)
            if replica is None:
                continue
            if replica.status is not ReplicaState.HEALTHY:
                raise InvalidState(
                    f"Cannot drain node {node_id}: replica {replica.replica_id} "
                    f"is {replica.status}; repair it before migration."
                )
            version = self._version(replica.version_id)
            target = self._eligible_target(
                version,
                excluded_node_ids={node_id},
            )
            results.append(
                await self.migrate_replica(
                    version.version_id,
                    source_node_id=node_id,
                    target_node_id=target.node_id,
                )
            )

        remaining = int(
            self.session.scalar(
                select(func.count(Replica.replica_id)).where(
                    Replica.node_id == node_id,
                )
            )
            or 0
        )
        if remaining == 0:
            self.metadata.transition_node_state(node_id, NodeState.REMOVED)

        return results

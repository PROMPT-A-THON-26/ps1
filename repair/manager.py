"""Durable, verified replica repair orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import ErrorCode, JobStatus, NodeState, ReplicaState
from common.errors import ObjectNotFound, VaultError
from common.ids import new_uuid
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, StorageNode, Version
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageNodeIntegrityError,
    StorageObjectAlreadyExistsError,
)


ClientFactory = type[StorageNodeClient]


@dataclass(frozen=True, slots=True)
class RepairResult:
    repair_id: UUID
    version_id: UUID
    source_node_id: str
    target_node_id: str
    status: JobStatus
    attempts: int


class RepairManager:
    """Create and execute durable repair jobs without deleting source data first."""

    def __init__(
        self,
        session: Session,
        *,
        client_factory=StorageNodeClient,
        max_attempts: int = 5,
    ) -> None:
        if not isinstance(max_attempts, int) or isinstance(max_attempts, bool) or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        self.session = session
        self.metadata = MetadataManager(session)
        self.client_factory = client_factory
        self.max_attempts = max_attempts

    def _version(self, version_id: UUID) -> Version:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        return version

    def _healthy_sources(self, version_id: UUID) -> list[Replica]:
        replicas = list(
            self.session.scalars(
                select(Replica)
                .where(
                    Replica.version_id == version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
                .order_by(
                    Replica.last_verified_at.desc(),
                    Replica.node_id.asc(),
                )
            ).all()
        )
        return [replica for replica in replicas if replica.checksum]

    def _target_for_version(
        self,
        version: Version,
        *,
        preferred_replica: Replica | None = None,
    ) -> StorageNode:
        # A corrupted replica on a reachable healthy node is repaired in place.
        if preferred_replica is not None:
            node = self.session.scalar(
                select(StorageNode).where(
                    StorageNode.node_id == preferred_replica.node_id
                )
            )
            if (
                node is not None
                and node.status is NodeState.HEALTHY
                and node.free_bytes >= version.size_bytes
            ):
                return node

        existing_node_ids = set(
            self.session.scalars(
                select(Replica.node_id).where(Replica.version_id == version.version_id)
            ).all()
        )
        candidates = list(
            self.session.scalars(
                select(StorageNode).where(StorageNode.status == NodeState.HEALTHY)
            ).all()
        )
        candidates = [
            node
            for node in candidates
            if node.node_id not in existing_node_ids
            and node.free_bytes >= version.size_bytes
        ]
        candidates.sort(key=lambda node: (-node.free_bytes, node.node_id))
        if not candidates:
            raise VaultError(
                code=ErrorCode.INSUFFICIENT_REPLICAS,
                message=f"No healthy target node can hold version {version.version_id}.",
                status_code=503,
            )
        return candidates[0]

    def _active_job_exists(self, version_id: UUID, target_node_id: str) -> bool:
        return (
            self.session.scalar(
                select(RepairJob.repair_id).where(
                    RepairJob.version_id == version_id,
                    RepairJob.target_node_id == target_node_id,
                    RepairJob.status.in_((JobStatus.PENDING, JobStatus.RUNNING)),
                )
            )
            is not None
        )

    def create_job(
        self,
        version_id: UUID,
        *,
        source_node_id: str,
        target_node_id: str,
        reason: str,
    ) -> RepairJob:
        if source_node_id == target_node_id:
            preferred = self.session.scalar(
                select(Replica).where(
                    Replica.version_id == version_id,
                    Replica.node_id == target_node_id,
                )
            )
            if preferred is None or preferred.status is not ReplicaState.CORRUPTED:
                raise ValueError(
                    "source and target may be the same node only for a corrupted replica repair"
                )

        if self._active_job_exists(version_id, target_node_id):
            raise VaultError(
                code=ErrorCode.REPAIR_IN_PROGRESS,
                message=(
                    f"Repair for version {version_id} on node {target_node_id} "
                    "is already in progress."
                ),
                status_code=409,
            )

        source = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == source_node_id)
        )
        target = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == target_node_id)
        )
        if source is None:
            raise ObjectNotFound(source_node_id)
        if target is None:
            raise ObjectNotFound(target_node_id)

        job = RepairJob(
            repair_id=new_uuid(),
            version_id=version_id,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            reason=reason.strip() if isinstance(reason, str) and reason.strip() else "under-replicated",
            status=JobStatus.PENDING,
            attempts=0,
        )
        self.session.add(job)
        self.session.commit()
        return job

    def schedule_for_version(
        self,
        version_id: UUID,
        *,
        replication_factor: int = 3,
        reason: str = "under-replicated",
        preferred_replica: Replica | None = None,
    ) -> RepairJob | None:
        if not isinstance(replication_factor, int) or isinstance(replication_factor, bool) or replication_factor < 1:
            raise ValueError("replication_factor must be a positive integer")

        version = self._version(version_id)
        healthy_count = int(
            self.session.scalar(
                select(func.count(Replica.replica_id)).where(
                    Replica.version_id == version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
            )
            or 0
        )
        if healthy_count >= replication_factor and preferred_replica is None:
            return None

        sources = self._healthy_sources(version_id)
        if not sources:
            raise VaultError(
                code=ErrorCode.NODE_UNAVAILABLE,
                message=f"No verified healthy source exists for version {version_id}.",
                status_code=503,
            )

        source = sources[0]
        target = self._target_for_version(
            version,
            preferred_replica=preferred_replica,
        )

        existing = self.session.scalar(
            select(RepairJob)
            .where(
                RepairJob.version_id == version_id,
                RepairJob.target_node_id == target.node_id,
                RepairJob.status.in_((JobStatus.PENDING, JobStatus.RUNNING)),
            )
        )
        if existing is not None:
            return existing

        return self.create_job(
            version_id,
            source_node_id=source.node_id,
            target_node_id=target.node_id,
            reason=reason,
        )

    def _replica_for_target(self, job: RepairJob) -> Replica:
        replica = self.session.scalar(
            select(Replica)
            .where(
                Replica.version_id == job.version_id,
                Replica.node_id == job.target_node_id,
            )
            .with_for_update()
        )
        if replica is None:
            replica = self.metadata.create_replica(job.version_id, job.target_node_id)
        if replica.status is ReplicaState.HEALTHY:
            raise VaultError(
                code=ErrorCode.REPAIR_IN_PROGRESS,
                message=f"Target node {job.target_node_id} is already healthy.",
                status_code=409,
            )
        if replica.status is ReplicaState.PENDING:
            self.metadata.set_replica_state(
                replica.replica_id, ReplicaState.REPAIRING
            )
        elif replica.status in {
            ReplicaState.CORRUPTED,
            ReplicaState.STALE,
            ReplicaState.UNAVAILABLE,
        }:
            self.metadata.set_replica_state(
                replica.replica_id, ReplicaState.REPAIRING
            )
        elif replica.status is not ReplicaState.REPAIRING:
            raise VaultError(
                code=ErrorCode.INVALID_REQUEST,
                message=(
                    f"Replica {replica.replica_id} cannot be used as a repair target "
                    f"from state {replica.status}."
                ),
                status_code=409,
            )
        return replica

    def _mark_job(self, job: RepairJob, status: JobStatus, *, error: str | None = None) -> None:
        job.status = status
        job.last_error = error
        job.updated_at = datetime.now(timezone.utc)
        self.session.commit()

    async def run_job(self, repair_id: UUID) -> RepairResult:
        job = self.session.scalar(
            select(RepairJob).where(RepairJob.repair_id == repair_id).with_for_update()
        )
        if job is None:
            raise ObjectNotFound(str(repair_id))
        if job.status is JobStatus.SUCCEEDED:
            return RepairResult(
                job.repair_id,
                job.version_id,
                job.source_node_id,
                job.target_node_id,
                job.status,
                job.attempts,
            )
        if job.status is JobStatus.FAILED and job.attempts >= self.max_attempts:
            return RepairResult(
                job.repair_id,
                job.version_id,
                job.source_node_id,
                job.target_node_id,
                job.status,
                job.attempts,
            )

        version = self._version(job.version_id)
        source_replica = self.session.scalar(
            select(Replica).where(
                Replica.version_id == job.version_id,
                Replica.node_id == job.source_node_id,
                Replica.status == ReplicaState.HEALTHY,
            )
        )
        if source_replica is None or not source_replica.checksum:
            self._mark_job(
                job,
                JobStatus.FAILED,
                error="Verified healthy source replica is unavailable.",
            )
            raise VaultError(
                code=ErrorCode.NODE_UNAVAILABLE,
                message="Verified healthy repair source is unavailable.",
                status_code=503,
            )

        target_replica = self._replica_for_target(job)
        target_node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == job.target_node_id)
        )
        source_node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == job.source_node_id)
        )
        if target_node is None or source_node is None:
            self._mark_job(job, JobStatus.FAILED, error="Repair source or target node missing.")
            raise ObjectNotFound(job.target_node_id if target_node is None else job.source_node_id)
        if target_node.status is not NodeState.HEALTHY:
            self._mark_job(job, JobStatus.FAILED, error="Repair target is not healthy.")
            raise VaultError(
                code=ErrorCode.NODE_UNAVAILABLE,
                message=f"Repair target node {target_node.node_id} is not healthy.",
                status_code=503,
            )

        job.status = JobStatus.RUNNING
        job.attempts += 1
        job.last_error = None
        self.session.commit()

        source_client = self.client_factory(source_node.address)
        target_client = self.client_factory(target_node.address)
        try:
            verified_source = await source_client.verify_object(
                str(version.object_id),
                str(version.version_id),
            )
            if (
                not verified_source.valid
                or verified_source.checksum.lower() != version.checksum.lower()
                or verified_source.size_bytes != version.size_bytes
            ):
                raise StorageNodeIntegrityError(
                    "Repair source verification does not match metadata."
                )

            try:
                async with source_client.stream_object(
                    str(version.object_id),
                    str(version.version_id),
                ) as source_response:
                    await target_client.put_object(
                        str(version.object_id),
                        str(version.version_id),
                        source_response.aiter_bytes(),
                    )
            except StorageObjectAlreadyExistsError:
                pass

            verified_target = await target_client.verify_object(
                str(version.object_id),
                str(version.version_id),
            )
            if (
                not verified_target.valid
                or verified_target.checksum.lower() != version.checksum.lower()
                or verified_target.size_bytes != version.size_bytes
            ):
                raise StorageNodeIntegrityError(
                    "Repair target verification does not match metadata."
                )

            self.metadata.mark_replica_healthy(
                target_replica.replica_id,
                checksum=verified_target.checksum,
                size_bytes=verified_target.size_bytes,
            )
            self._mark_job(job, JobStatus.SUCCEEDED)
            return RepairResult(
                job.repair_id,
                job.version_id,
                job.source_node_id,
                job.target_node_id,
                job.status,
                job.attempts,
            )
        except StorageNodeClientError as exc:
            error = str(exc)
            self._mark_job(
                job,
                JobStatus.FAILED if job.attempts >= self.max_attempts else JobStatus.PENDING,
                error=error,
            )
            raise
        except VaultError as exc:
            self._mark_job(
                job,
                JobStatus.FAILED if job.attempts >= self.max_attempts else JobStatus.PENDING,
                error=exc.message,
            )
            raise
        finally:
            await source_client.aclose()
            await target_client.aclose()

    async def repair_version_until_healthy(
        self,
        version_id: UUID,
        *,
        replication_factor: int = 3,
        reason: str = "under-replicated",
    ) -> list[RepairResult]:
        results: list[RepairResult] = []
        while True:
            healthy_count = int(
                self.session.scalar(
                    select(func.count(Replica.replica_id)).where(
                        Replica.version_id == version_id,
                        Replica.status == ReplicaState.HEALTHY,
                    )
                )
                or 0
            )
            if healthy_count >= replication_factor:
                return results

            job = self.schedule_for_version(
                version_id,
                replication_factor=replication_factor,
                reason=reason,
            )
            if job is None:
                return results
            results.append(await self.run_job(job.repair_id))

"""Automatic storage rebalancing for Vault.

Rebalancing moves an already healthy replica from an overloaded node to a
healthier node. The destination is fully copied and independently verified
before the source is deleted.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping
from uuid import UUID

from sqlalchemy import select

from common.constants import JobStatus, NodeState, ReplicaState, VersionState
from common.errors import InsufficientReplicas, InvalidState, ObjectNotFound
from metadata.manager import MetadataManager
from metadata.models import RebalanceJob, Replica, StorageNode, Version
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageNodeIntegrityError,
)


@dataclass(frozen=True, slots=True)
class RebalancePolicy:
    """Utilization thresholds and per-scan work limit."""

    high_watermark: float = 0.80
    low_watermark: float = 0.60
    max_moves_per_scan: int = 8

    def __post_init__(self) -> None:
        if not 0 < self.low_watermark < self.high_watermark < 1:
            raise ValueError(
                "watermarks must satisfy 0 < low_watermark < high_watermark < 1"
            )
        if self.max_moves_per_scan < 1:
            raise ValueError("max_moves_per_scan must be at least 1")


@dataclass(frozen=True, slots=True)
class RebalanceResult:
    scanned_nodes: int
    planned_jobs: int
    completed_jobs: int
    failed_jobs: int


class Rebalancer:
    """Plan and execute safe replica moves using durable metadata jobs."""

    def __init__(
        self,
        session,
        nodes: Mapping[str, StorageNodeClient],
        *,
        replication_factor: int = 3,
        policy: RebalancePolicy | None = None,
    ) -> None:
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        self.session = session
        self.manager = MetadataManager(session)
        self.nodes = dict(nodes)
        self.replication_factor = replication_factor
        self.policy = policy or RebalancePolicy()

    async def run_once(self) -> RebalanceResult:
        scanned = await self._refresh_nodes()
        jobs = self._plan_moves()
        completed = 0
        failed = 0
        for job in jobs:
            try:
                await self.execute_job(job.rebalance_id)
            except Exception:
                failed += 1
            else:
                completed += 1
        return RebalanceResult(
            scanned_nodes=scanned,
            planned_jobs=len(jobs),
            completed_jobs=completed,
            failed_jobs=failed,
        )

    async def _refresh_nodes(self) -> int:
        scanned = 0
        for node_id, client in self.nodes.items():
            try:
                health = await client.health()
                if health.status.lower() != NodeState.HEALTHY.value.lower():
                    continue
                stats = await client.stats()
                self.manager.update_node_heartbeat(
                    node_id,
                    capacity_bytes=stats.capacity_bytes,
                    used_bytes=stats.used_bytes,
                    status=NodeState.HEALTHY,
                )
                scanned += 1
            except StorageNodeClientError:
                # The failure detector owns liveness transitions. Rebalancing
                # simply excludes nodes it cannot currently inspect.
                continue
        return scanned

    def _plan_moves(self) -> list[RebalanceJob]:
        nodes = list(
            self.session.scalars(
                select(StorageNode).where(StorageNode.status == NodeState.HEALTHY)
            )
        )
        overloaded = [
            node for node in nodes
            if node.capacity_bytes > 0
            and node.used_bytes / node.capacity_bytes > self.policy.high_watermark
        ]
        targets = [
            node for node in nodes
            if node.capacity_bytes > 0
            and node.used_bytes / node.capacity_bytes < self.policy.low_watermark
        ]
        overloaded.sort(
            key=lambda node: (-node.used_bytes / node.capacity_bytes, node.node_id)
        )
        targets.sort(
            key=lambda node: (node.used_bytes / node.capacity_bytes, node.node_id)
        )

        planned: list[RebalanceJob] = []
        for source in overloaded:
            replicas = list(
                self.session.scalars(
                    select(Replica)
                    .join(Version, Replica.version_id == Version.version_id)
                    .where(
                        Replica.node_id == source.node_id,
                        Replica.status == ReplicaState.HEALTHY,
                        Version.state == VersionState.COMMITTED,
                    )
                    .order_by(Replica.created_at, Replica.replica_id)
                )
            )
            for replica in replicas:
                if len(planned) >= self.policy.max_moves_per_scan:
                    return planned
                version = self.session.scalar(
                    select(Version).where(Version.version_id == replica.version_id)
                )
                if version is None:
                    continue
                all_replicas = list(
                    self.session.scalars(
                        select(Replica).where(Replica.version_id == version.version_id)
                    )
                )
                healthy_count = sum(
                    item.status is ReplicaState.HEALTHY for item in all_replicas
                )
                if healthy_count < self.replication_factor:
                    # Durability repair takes precedence over rebalancing.
                    continue
                existing_nodes = {item.node_id for item in all_replicas}
                target = next(
                    (
                        candidate for candidate in targets
                        if candidate.node_id not in existing_nodes
                        and candidate.free_bytes >= version.size_bytes
                    ),
                    None,
                )
                if target is None:
                    continue
                duplicate = self.session.scalar(
                    select(RebalanceJob).where(
                        RebalanceJob.version_id == version.version_id,
                        RebalanceJob.source_node_id == source.node_id,
                        RebalanceJob.target_node_id == target.node_id,
                        RebalanceJob.status.in_(
                            [JobStatus.PENDING, JobStatus.RUNNING]
                        ),
                    )
                )
                if duplicate is not None:
                    continue
                job = RebalanceJob(
                    version_id=version.version_id,
                    source_node_id=source.node_id,
                    target_node_id=target.node_id,
                    status=JobStatus.PENDING,
                    attempts=0,
                )
                self.session.add(job)
                self.session.flush()
                planned.append(job)
        return planned

    async def execute_job(self, rebalance_id: UUID) -> RebalanceJob:
        job = self.session.scalar(
            select(RebalanceJob)
            .where(RebalanceJob.rebalance_id == rebalance_id)
            .with_for_update()
        )
        if job is None:
            raise ObjectNotFound(str(rebalance_id))
        if job.status is JobStatus.SUCCEEDED:
            return job
        if job.status is not JobStatus.PENDING:
            raise InvalidState(
                f"Rebalance job {rebalance_id} is {job.status}, not PENDING."
            )

        job.status = JobStatus.RUNNING
        job.attempts += 1
        self.session.flush()
        try:
            await self._execute_running_job(job)
        except Exception as exc:
            job.status = JobStatus.FAILED
            job.last_error = str(exc)
            self.session.flush()
            raise
        job.status = JobStatus.SUCCEEDED
        job.last_error = None
        self.session.flush()
        return job

    async def _execute_running_job(self, job: RebalanceJob) -> None:
        version = self.session.scalar(
            select(Version).where(Version.version_id == job.version_id)
        )
        if version is None:
            raise ObjectNotFound(str(job.version_id))
        if version.state is not VersionState.COMMITTED:
            raise InvalidState(f"Version {version.version_id} is not committed.")

        source = self.nodes.get(job.source_node_id)
        target = self.nodes.get(job.target_node_id)
        if source is None or target is None:
            raise ObjectNotFound("rebalance source or target node client")

        source_replica = self.session.scalar(
            select(Replica)
            .where(
                Replica.version_id == version.version_id,
                Replica.node_id == job.source_node_id,
            )
            .with_for_update()
        )
        if source_replica is None or source_replica.status is not ReplicaState.HEALTHY:
            raise InvalidState("rebalance source replica is not healthy")

        target_replica = self.session.scalar(
            select(Replica)
            .where(
                Replica.version_id == version.version_id,
                Replica.node_id == job.target_node_id,
            )
            .with_for_update()
        )
        if target_replica is None:
            target_replica = self.manager.create_replica(
                version.version_id, job.target_node_id
            )
            self.manager.set_replica_state(
                target_replica.replica_id, ReplicaState.COPYING
            )
        elif target_replica.status is not ReplicaState.HEALTHY:
            if target_replica.status is ReplicaState.FAILED:
                target_replica.status = ReplicaState.COPYING
                self.session.flush()
            elif target_replica.status is ReplicaState.PENDING:
                self.manager.set_replica_state(
                    target_replica.replica_id, ReplicaState.COPYING
                )
            else:
                raise InvalidState(
                    f"rebalance target replica is {target_replica.status}"
                )

        await source.health()
        await target.health()

        verified_source = await source.verify_object(
            str(version.object_id), str(version.version_id)
        )
        if (
            verified_source.checksum != version.checksum
            or verified_source.size_bytes != version.size_bytes
        ):
            raise StorageNodeIntegrityError(
                "rebalance source does not match canonical version metadata"
            )

        if target_replica.status is not ReplicaState.HEALTHY:
            async with source.stream_object(
                str(version.object_id), str(version.version_id)
            ) as response:
                async def stream():
                    async for chunk in response.aiter_bytes():
                        yield chunk

                await target.put_object(
                    str(version.object_id), str(version.version_id), stream()
                )

            verified_target = await target.verify_object(
                str(version.object_id), str(version.version_id)
            )
            if (
                verified_target.checksum != version.checksum
                or verified_target.size_bytes != version.size_bytes
            ):
                raise StorageNodeIntegrityError(
                    "rebalance destination failed canonical verification"
                )
            self.manager.mark_replica_healthy(
                target_replica.replica_id,
                checksum=verified_target.checksum,
                size_bytes=verified_target.size_bytes,
            )
        else:
            verified_target = await target.verify_object(
                str(version.object_id), str(version.version_id)
            )
            if (
                verified_target.checksum != version.checksum
                or verified_target.size_bytes != version.size_bytes
            ):
                raise StorageNodeIntegrityError(
                    "existing rebalance destination failed canonical verification"
                )

        healthy_replicas = list(
            self.session.scalars(
                select(Replica).where(
                    Replica.version_id == version.version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
            )
        )
        # Source must remain until the destination is verified and the healthy
        # set has increased above the configured durability floor.
        if len(healthy_replicas) < self.replication_factor + 1:
            raise InsufficientReplicas(
                self.replication_factor + 1, len(healthy_replicas)
            )

        await source.delete_object(
            str(version.object_id), str(version.version_id)
        )
        self.session.delete(source_replica)
        self.session.flush()


__all__ = ["RebalancePolicy", "RebalanceResult", "Rebalancer"]

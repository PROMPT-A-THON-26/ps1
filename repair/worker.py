"""Durable background replica-repair worker."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import JobStatus, ReplicaState, VersionState
from common.errors import InsufficientReplicas, InvalidState, ObjectNotFound
from metadata.manager import MetadataManager
from metadata.models import RepairJob, Replica, Version
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import StorageNodeClientError


@dataclass(frozen=True, slots=True)
class RepairWorkerConfig:
    """Polling policy for automatic replica repair."""

    interval_seconds: float = 5.0
    batch_size: int = 16

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1")


@dataclass(frozen=True, slots=True)
class RepairRunResult:
    scheduled: int
    succeeded: int
    failed: int
    skipped: int


class RepairWorker:
    """Discover replica deficits, persist repair jobs, and execute them safely.

    The SQLAlchemy session is synchronous and is intentionally shared by the
    coordinator and worker, so jobs are executed sequentially in one worker
    process. Horizontal concurrency should use separate worker processes and
    sessions rather than concurrent coroutines over one Session.
    """

    REPAIRABLE_REPLICA_STATES: Final[frozenset[ReplicaState]] = frozenset(
        {
            ReplicaState.UNAVAILABLE,
            ReplicaState.CORRUPTED,
            ReplicaState.STALE,
            ReplicaState.FAILED,
        }
    )

    def __init__(
        self,
        session: Session,
        coordinator: DistributedWriteCoordinator,
        *,
        config: RepairWorkerConfig | None = None,
    ) -> None:
        self.session = session
        self.manager = MetadataManager(session)
        self.coordinator = coordinator
        self.config = config or RepairWorkerConfig()

    async def run_once(self) -> RepairRunResult:
        scheduled = await self._schedule_missing_repairs()
        succeeded = 0
        failed = 0
        skipped = 0

        jobs = list(
            self.session.scalars(
                select(RepairJob)
                .where(RepairJob.status == JobStatus.PENDING)
                .order_by(RepairJob.created_at)
                .limit(self.config.batch_size)
            )
        )

        for job in jobs:
            try:
                await self._execute_job(job.repair_id)
            except Exception:
                failed += 1
            else:
                succeeded += 1

        # A scheduling failure is deliberately separated from job execution
        # failure so a temporary lack of replacement capacity does not create a
        # false successful repair result.
        skipped += scheduled.skipped
        return RepairRunResult(
            scheduled=scheduled.scheduled,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
        )

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Poll for deficits until stop_event is set."""
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.config.interval_seconds,
                )
            except asyncio.TimeoutError:
                continue

    async def _schedule_missing_repairs(self) -> RepairRunResult:
        scheduled = 0
        skipped = 0

        versions = list(
            self.session.scalars(
                select(Version)
                .where(Version.state == VersionState.COMMITTED)
                .order_by(Version.created_at)
            )
        )

        for version in versions:
            replicas = list(
                self.session.scalars(
                    select(Replica).where(Replica.version_id == version.version_id)
                )
            )
            healthy_count = sum(
                replica.status is ReplicaState.HEALTHY for replica in replicas
            )
            if healthy_count >= self.coordinator.replication_factor:
                continue

            failed = next(
                (
                    replica
                    for replica in replicas
                    if replica.status in self.REPAIRABLE_REPLICA_STATES
                ),
                None,
            )
            if failed is None:
                skipped += 1
                continue

            active_job = self.session.scalar(
                select(RepairJob)
                .where(
                    RepairJob.version_id == version.version_id,
                    RepairJob.status.in_(
                        [JobStatus.PENDING, JobStatus.RUNNING]
                    ),
                )
            )
            if active_job is not None:
                continue

            try:
                plan = await self.coordinator.plan_repair(
                    version.version_id,
                    failed.node_id,
                )
                self.manager.create_repair_job(
                    version_id=version.version_id,
                    failed_node_id=failed.node_id,
                    source_node_id=plan.source_node_id,
                    target_node_id=plan.target_node_id,
                    reason=f"replica deficit after node/replica state {failed.status}",
                )
            except (
                InsufficientReplicas,
                InvalidState,
                ObjectNotFound,
                StorageNodeClientError,
            ):
                skipped += 1
                continue

            scheduled += 1

        return RepairRunResult(
            scheduled=scheduled,
            succeeded=0,
            failed=0,
            skipped=skipped,
        )

    async def _execute_job(self, repair_id):
        job = self.manager.claim_repair_job(repair_id)
        try:
            await self.coordinator.repair_version(
                job.version_id,
                job.failed_node_id,
                source_node_id=job.source_node_id,
                target_node_id=job.target_node_id,
            )
        except Exception as exc:
            try:
                self.manager.fail_repair_job(repair_id, self._error_text(exc))
            except Exception:
                # Preserve the original failure so the caller knows the
                # repair itself did not succeed.
                pass
            raise
        else:
            self.manager.complete_repair_job(repair_id)

    @staticmethod
    def _error_text(error: Exception) -> str:
        message = str(error).strip()
        if message:
            return f"{type(error).__name__}: {message}"[:4000]
        return type(error).__name__


__all__ = [
    "RepairRunResult",
    "RepairWorker",
    "RepairWorkerConfig",
]

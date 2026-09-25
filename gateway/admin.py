"""Administrative gateway orchestration for durable background operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from celery.result import AsyncResult
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from common.config import get_settings
from common.constants import JobStatus
from common.errors import ObjectNotFound, VaultError
from metadata.models import RebalanceJob, RepairJob, Replica
from rebalance import RebalanceManager
from repair import RepairManager
from worker.celery_app import celery_app
from worker.tasks import (
    migrate_replica,
    rebalance_node,
    run_rebalance_job,
    run_repair_job,
    scan_node,
    scan_version,
    verify_replica,
)


class RepairRequest(BaseModel):
    version_id: UUID
    replication_factor: int | None = Field(default=None, ge=1)
    reason: str = Field(default="admin-request", min_length=1, max_length=200)


class IntegrityRequest(BaseModel):
    replica_id: UUID | None = None
    node_id: str | None = None
    version_id: UUID | None = None

    @model_validator(mode="after")
    def exactly_one_target(self):
        if sum(value is not None for value in (self.replica_id, self.node_id, self.version_id)) != 1:
            raise ValueError("Exactly one of replica_id, node_id, or version_id is required.")
        return self


class RebalanceRequest(BaseModel):
    node_id: str | None = None
    replica_id: UUID | None = None
    target_node_id: str | None = None

    @model_validator(mode="after")
    def validate_target(self):
        if self.node_id is not None:
            if self.replica_id is not None or self.target_node_id is not None:
                raise ValueError("node_id cannot be combined with replica_id/target_node_id.")
            return self
        if self.replica_id is None or not self.target_node_id:
            raise ValueError("Provide node_id for a drain or replica_id + target_node_id for migration.")
        return self


@dataclass(frozen=True, slots=True)
class AdminDispatch:
    job_id: str
    task_id: str
    status: str


def _celery_status(task_id: str) -> str:
    state = AsyncResult(task_id, app=celery_app).state
    return {
        "PENDING": JobStatus.PENDING.value,
        "STARTED": JobStatus.RUNNING.value,
        "RETRY": JobStatus.PENDING.value,
        "SUCCESS": JobStatus.SUCCEEDED.value,
        "FAILURE": JobStatus.FAILED.value,
    }.get(state, state)


class AdminService:
    """Business logic for administrative repair/integrity/rebalance endpoints."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def enqueue_repair(self, request: RepairRequest) -> AdminDispatch:
        manager = RepairManager(
            self.session,
            replication_factor=request.replication_factor or get_settings().replication_factor,
            max_attempts=get_settings().max_attempts,
        )
        job = manager.schedule_for_version(
            request.version_id,
            replication_factor=request.replication_factor or get_settings().replication_factor,
            reason=request.reason,
        )
        if job is None:
            raise VaultError(
                code="REPAIR_IN_PROGRESS",
                message=f"Version {request.version_id} already satisfies the requested replication factor.",
                status_code=409,
            )
        async_result = run_repair_job.delay(str(job.repair_id))
        return AdminDispatch(
            job_id=str(job.repair_id),
            task_id=async_result.id,
            status=job.status.value,
        )

    def repair_status(self, repair_id: UUID) -> dict[str, Any]:
        job = self.session.scalar(
            select(RepairJob).where(RepairJob.repair_id == repair_id)
        )
        if job is None:
            raise ObjectNotFound(str(repair_id))
        return {
            "repair_id": str(job.repair_id),
            "version_id": str(job.version_id),
            "source_node_id": job.source_node_id,
            "target_node_id": job.target_node_id,
            "reason": job.reason,
            "status": job.status.value,
            "attempts": job.attempts,
            "last_error": job.last_error,
            "created_at": job.created_at,
            "updated_at": job.updated_at,
        }

    def enqueue_integrity(self, request: IntegrityRequest) -> AdminDispatch:
        if request.replica_id is not None:
            result = verify_replica.delay(str(request.replica_id))
            return AdminDispatch(str(request.replica_id), result.id, _celery_status(result.id))
        if request.node_id is not None:
            result = scan_node.delay(request.node_id)
            return AdminDispatch(request.node_id, result.id, _celery_status(result.id))
        assert request.version_id is not None
        result = scan_version.delay(str(request.version_id))
        return AdminDispatch(str(request.version_id), result.id, _celery_status(result.id))

    def enqueue_rebalance(self, request: RebalanceRequest) -> AdminDispatch:
        if request.node_id is not None:
            result = rebalance_node.delay(request.node_id)
            return AdminDispatch(request.node_id, result.id, _celery_status(result.id))

        assert request.replica_id is not None and request.target_node_id is not None
        replica = self.session.scalar(
            select(Replica).where(Replica.replica_id == request.replica_id)
        )
        if replica is None:
            raise ObjectNotFound(str(request.replica_id))

        manager = RebalanceManager(
            self.session,
            replication_factor=get_settings().replication_factor,
            max_attempts=get_settings().max_attempts,
        )
        existing = self.session.scalar(
            select(RebalanceJob).where(
                RebalanceJob.version_id == replica.version_id,
                RebalanceJob.source_node_id == replica.node_id,
                RebalanceJob.target_node_id == request.target_node_id,
                RebalanceJob.status.in_((JobStatus.PENDING, JobStatus.RUNNING)),
            )
        )
        job = existing or manager.create_job(
            replica.version_id,
            source_node_id=replica.node_id,
            target_node_id=request.target_node_id,
        )
        result = run_rebalance_job.delay(str(job.rebalance_id))
        return AdminDispatch(str(job.rebalance_id), result.id, job.status.value)

    def job_status(self, job_id: str, *, kind: str) -> dict[str, Any]:
        if kind == "repair":
            try:
                repair_id = UUID(job_id)
            except ValueError:
                raise ObjectNotFound(job_id)
            return self.repair_status(repair_id)

        if kind == "rebalance":
            try:
                rebalance_id = UUID(job_id)
            except ValueError:
                rebalance_id = None
            if rebalance_id is not None:
                job = self.session.scalar(
                    select(RebalanceJob).where(
                        RebalanceJob.rebalance_id == rebalance_id
                    )
                )
                if job is not None:
                    return {
                        "rebalance_id": str(job.rebalance_id),
                        "version_id": str(job.version_id),
                        "source_node_id": job.source_node_id,
                        "target_node_id": job.target_node_id,
                        "status": job.status.value,
                        "attempts": job.attempts,
                        "last_error": job.last_error,
                        "created_at": job.created_at,
                        "updated_at": job.updated_at,
                    }

        return {
            "job_id": job_id,
            "task_id": job_id,
            "status": _celery_status(job_id),
        }

    def integrity_status(self, job_id: str) -> dict[str, Any]:
        return {
            "job_id": job_id,
            "task_id": job_id,
            "status": _celery_status(job_id),
        }


def dispatch_to_payload(dispatch: AdminDispatch) -> dict[str, str]:
    return {
        "job_id": dispatch.job_id,
        "task_id": dispatch.task_id,
        "status": dispatch.status,
    }


__all__ = [
    "AdminService",
    "AdminDispatch",
    "IntegrityRequest",
    "RepairRequest",
    "RebalanceRequest",
    "dispatch_to_payload",
]

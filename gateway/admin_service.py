"""Administrative control-plane API service for durable background jobs."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import JobStatus
from common.errors import InvalidState, ObjectNotFound
from integrity import IntegrityManager
from metadata.models import IntegrityJob, RebalanceJob, RepairJob, Replica
from rebalance import RebalanceManager
from repair import RepairManager


class RepairRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version_id: UUID
    source_node_id: str | None = Field(default=None, min_length=1)
    target_node_id: str | None = Field(default=None, min_length=1)
    reason: str = Field(default="admin-request", min_length=1)


class IntegrityRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    node_id: str | None = Field(default=None, min_length=1)
    version_id: UUID | None = None


class RebalanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version_id: UUID
    source_node_id: str = Field(min_length=1)
    target_node_id: str = Field(min_length=1)


def _dispatch(task_name: str, *args: str, **kwargs: str) -> dict[str, Any]:
    try:
        from worker import tasks
        result = getattr(tasks, task_name).apply_async(args=list(args), kwargs=kwargs)
        return {"queued": True, "task_id": str(result.id)}
    except Exception as exc:
        return {"queued": False, "task_id": None, "dispatch_error": str(exc)}


def _serialize_job(job: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in (
        "repair_id", "rebalance_id", "integrity_id", "version_id",
        "source_node_id", "target_node_id", "node_id", "status",
        "attempts", "last_error", "reason", "checked_count",
        "corrupted_count", "created_at", "updated_at",
    ):
        if not hasattr(job, field):
            continue
        value = getattr(job, field)
        if isinstance(value, UUID):
            value = str(value)
        elif isinstance(value, JobStatus):
            value = value.value
        elif hasattr(value, "isoformat"):
            value = value.isoformat()
        result[field] = value
    return result


class AdminService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_repair(self, request: RepairRequest) -> dict[str, Any]:
        both = request.source_node_id is not None and request.target_node_id is not None
        one = request.source_node_id is not None or request.target_node_id is not None
        if one and not both:
            raise ValueError("source_node_id and target_node_id must be supplied together")
        manager = RepairManager(self.session)
        job = (
            manager.create_job(
                request.version_id,
                source_node_id=request.source_node_id,
                target_node_id=request.target_node_id,
                reason=request.reason,
            )
            if both
            else manager.schedule_for_version(
                request.version_id, reason=request.reason
            )
        )
        if job is None:
            raise InvalidState("Version already satisfies the replication policy.")
        return {
            **_serialize_job(job),
            **_dispatch("repair_version", str(request.version_id), repair_id=str(job.repair_id)),
        }

    def get_repair(self, repair_id: UUID) -> dict[str, Any]:
        job = self.session.scalar(select(RepairJob).where(RepairJob.repair_id == repair_id))
        if job is None:
            raise ObjectNotFound(str(repair_id))
        return _serialize_job(job)

    def create_integrity(self, request: IntegrityRequest) -> dict[str, Any]:
        job = IntegrityManager(self.session).create_job(
            node_id=request.node_id, version_id=request.version_id
        )
        return {
            **_serialize_job(job),
            **_dispatch("run_integrity_check", str(job.integrity_id)),
        }

    def get_integrity(self, integrity_id: UUID) -> dict[str, Any]:
        job = self.session.scalar(select(IntegrityJob).where(IntegrityJob.integrity_id == integrity_id))
        if job is None:
            raise ObjectNotFound(str(integrity_id))
        return _serialize_job(job)

    def create_rebalance(self, request: RebalanceRequest) -> dict[str, Any]:
        manager = RebalanceManager(self.session)
        job = manager.create_job(
            request.version_id,
            source_node_id=request.source_node_id,
            target_node_id=request.target_node_id,
        )
        replica = self.session.scalar(
            select(Replica).where(
                Replica.version_id == request.version_id,
                Replica.node_id == request.source_node_id,
            )
        )
        if replica is None:
            raise ObjectNotFound(
                f"Replica for version {request.version_id} on {request.source_node_id}"
            )
        return {
            **_serialize_job(job),
            **_dispatch(
                "migrate_replica",
                str(replica.replica_id),
                target_node_id=request.target_node_id,
                rebalance_id=str(job.rebalance_id),
            ),
        }

    def get_rebalance(self, rebalance_id: UUID) -> dict[str, Any]:
        job = self.session.scalar(
            select(RebalanceJob).where(RebalanceJob.rebalance_id == rebalance_id)
        )
        if job is None:
            raise ObjectNotFound(str(rebalance_id))
        return _serialize_job(job)

"""Durable Celery entrypoints for repair, integrity, health, and rebalancing."""

from __future__ import annotations

import asyncio
from uuid import UUID

from celery import Task
from sqlalchemy import select

from common.config import get_settings
from common.constants import JobStatus, NodeState, ReplicaState, VersionState
from common.errors import ObjectNotFound, VaultError
from health.failure_detector import FailureDetector
from integrity import IntegrityManager
from metadata.database import SessionLocal
from metadata.models import RebalanceJob, RepairJob, Replica, StorageNode, Version
from rebalance import RebalanceManager
from repair import RepairManager
from replication.node_client import StorageNodeClientError

from .celery_app import celery_app


def _run(coro):
    """Run one async manager operation inside a synchronous Celery worker."""
    return asyncio.run(coro)


def _retryable(error: BaseException) -> bool:
    if isinstance(error, StorageNodeClientError):
        return True
    if isinstance(error, VaultError):
        return getattr(error, "code", None) in {
            "NODE_UNAVAILABLE",
            "INSUFFICIENT_REPLICAS",
            "REPAIR_IN_PROGRESS",
        }
    return False


def _retry(task: Task, error: BaseException):
    settings = get_settings()
    attempt = int(task.request.retries)
    if attempt >= settings.max_attempts - 1:
        raise error
    delay = settings.initial_backoff_seconds * (2**attempt)
    raise task.retry(exc=error, countdown=delay)


def _job_payload(job) -> dict[str, object]:
    job_id = getattr(job, "repair_id", None) or getattr(job, "rebalance_id", None)
    return {
        "job_id": str(job_id),
        "status": job.status.value,
        "attempts": job.attempts,
    }


@celery_app.task(
    bind=True,
    name="repair_version",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def repair_version(task: Task, version_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            results = _run(
                RepairManager(
                    session,
                    replication_factor=get_settings().replication_factor,
                    max_attempts=get_settings().max_attempts,
                ).repair_version_until_healthy(
                    UUID(str(version_id)),
                    replication_factor=get_settings().replication_factor,
                )
            )
        return {
            "version_id": str(version_id),
            "jobs": [_job_payload(result) for result in results],
        }
    except (StorageNodeClientError, VaultError) as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="run_repair_job",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def run_repair_job(task: Task, repair_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = _run(
                RepairManager(
                    session,
                    max_attempts=get_settings().max_attempts,
                    replication_factor=get_settings().replication_factor,
                ).run_job(UUID(str(repair_id)))
            )
        return _job_payload(result)
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="verify_replica",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def verify_replica(task: Task, replica_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = _run(
                IntegrityManager(
                    session,
                    replication_factor=get_settings().replication_factor,
                ).verify_replica(UUID(str(replica_id)))
            )
        return {
            "replica_id": str(result.replica_id),
            "version_id": str(result.version_id),
            "node_id": result.node_id,
            "checked": result.checked,
            "corrupted": result.corrupted,
            "repair_id": None if result.repair_id is None else str(result.repair_id),
        }
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="scan_node",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def scan_node(task: Task, node_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            results = _run(
                IntegrityManager(
                    session,
                    replication_factor=get_settings().replication_factor,
                ).scan_node(node_id)
            )
        return {
            "node_id": node_id,
            "checked": sum(item.checked for item in results),
            "corrupted": sum(item.corrupted for item in results),
            "repair_ids": [
                str(item.repair_id) for item in results if item.repair_id is not None
            ],
        }
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="scan_version",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def scan_version(task: Task, version_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            results = _run(
                IntegrityManager(
                    session,
                    replication_factor=get_settings().replication_factor,
                ).scan_version(UUID(str(version_id)))
            )
        return {
            "version_id": version_id,
            "checked": sum(item.checked for item in results),
            "corrupted": sum(item.corrupted for item in results),
            "repair_ids": [
                str(item.repair_id) for item in results if item.repair_id is not None
            ],
        }
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="check_under_replicated_objects",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def check_under_replicated_objects(task: Task) -> dict[str, object]:
    failures: list[dict[str, str]] = []
    repaired_versions = 0
    with SessionLocal() as session:
        version_ids = list(
            session.scalars(
                select(Version.version_id).where(
                    Version.state == VersionState.COMMITTED
                )
            ).all()
        )

        for version_id in version_ids:
            try:
                results = _run(
                    RepairManager(
                        session,
                        replication_factor=get_settings().replication_factor,
                        max_attempts=get_settings().max_attempts,
                    ).repair_version_until_healthy(
                        version_id,
                        replication_factor=get_settings().replication_factor,
                        reason="background-under-replicated",
                    )
                )
                if results:
                    repaired_versions += 1
                    for result in results:
                        if result.status is not JobStatus.SUCCEEDED:
                            failures.append(
                                {
                                    "version_id": str(version_id),
                                    "repair_id": str(result.repair_id),
                                }
                            )
            except Exception as exc:
                if _retryable(exc):
                    failures.append(
                        {"version_id": str(version_id), "error": str(exc)}
                    )
                    continue
                raise

    return {
        "scanned_versions": len(version_ids),
        "repaired_versions": repaired_versions,
        "failures": failures,
    }


@celery_app.task(
    bind=True,
    name="rebalance_node",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def rebalance_node(task: Task, node_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            results = _run(
                RebalanceManager(
                    session,
                    replication_factor=get_settings().replication_factor,
                    max_attempts=get_settings().max_attempts,
                ).drain_node(node_id)
            )
        return {
            "node_id": node_id,
            "migrations": [_job_payload(result) for result in results],
        }
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="run_rebalance_job",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def run_rebalance_job(task: Task, rebalance_id: str) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            result = _run(
                RebalanceManager(
                    session,
                    replication_factor=get_settings().replication_factor,
                    max_attempts=get_settings().max_attempts,
                ).run_job(UUID(str(rebalance_id)))
            )
        return _job_payload(result)
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="migrate_replica",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def migrate_replica(
    task: Task,
    replica_id: str,
    target_node_id: str,
) -> dict[str, object]:
    try:
        with SessionLocal() as session:
            replica = session.scalar(
                select(Replica).where(Replica.replica_id == UUID(str(replica_id)))
            )
            if replica is None:
                raise ObjectNotFound(replica_id)
            manager = RebalanceManager(
                session,
                replication_factor=get_settings().replication_factor,
                max_attempts=get_settings().max_attempts,
            )
            existing = session.scalar(
                select(RebalanceJob).where(
                    RebalanceJob.version_id == replica.version_id,
                    RebalanceJob.source_node_id == replica.node_id,
                    RebalanceJob.target_node_id == target_node_id,
                    RebalanceJob.status.in_((JobStatus.PENDING, JobStatus.RUNNING)),
                )
            )
            job = existing or manager.create_job(
                replica.version_id,
                source_node_id=replica.node_id,
                target_node_id=target_node_id,
            )
            result = _run(manager.run_job(job.rebalance_id))
        return _job_payload(result)
    except Exception as exc:
        if _retryable(exc):
            return _retry(task, exc)
        raise


@celery_app.task(
    bind=True,
    name="process_node_health",
    max_retries=max(0, get_settings().max_attempts - 1),
)
def process_node_health(task: Task) -> dict[str, object]:
    with SessionLocal() as session:
        transitions = FailureDetector(
            session,
            suspect_after_seconds=get_settings().suspect_after_seconds,
            unavailable_after_seconds=get_settings().unavailable_after_seconds,
            replication_factor=get_settings().replication_factor,
        ).scan()

    dispatched: list[str] = []
    for transition in transitions:
        for repair_id in transition.scheduled_repair_ids:
            run_repair_job.delay(str(repair_id))
            dispatched.append(str(repair_id))

    return {
        "transitions": [
            {
                "node_id": transition.node_id,
                "previous": transition.previous.value,
                "current": transition.current.value,
                "scheduled_repair_ids": [
                    str(repair_id) for repair_id in transition.scheduled_repair_ids
                ],
            }
            for transition in transitions
        ],
        "dispatched_repair_ids": dispatched,
    }


@celery_app.task(name="scan_all_nodes")
def scan_all_nodes() -> dict[str, object]:
    with SessionLocal() as session:
        node_ids = list(
            session.scalars(
                select(StorageNode.node_id).where(StorageNode.status == NodeState.HEALTHY)
            ).all()
        )
    task_ids = [scan_node.delay(node_id).id for node_id in node_ids]
    return {"node_ids": node_ids, "task_ids": task_ids}


@celery_app.task(name="rebalance_draining_nodes")
def rebalance_draining_nodes() -> dict[str, object]:
    with SessionLocal() as session:
        node_ids = list(
            session.scalars(
                select(StorageNode.node_id).where(StorageNode.status == NodeState.DRAINING)
            ).all()
        )
    task_ids = [rebalance_node.delay(node_id).id for node_id in node_ids]
    return {"node_ids": node_ids, "task_ids": task_ids}


__all__ = [
    "check_under_replicated_objects",
    "migrate_replica",
    "process_node_health",
    "rebalance_node",
    "rebalance_draining_nodes",
    "repair_version",
    "run_rebalance_job",
    "run_repair_job",
    "scan_all_nodes",
    "scan_node",
    "scan_version",
    "verify_replica",
]

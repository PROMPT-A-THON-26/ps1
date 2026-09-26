"""Canonical retryable Celery tasks for the Vault control plane."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from celery import Task
from pydantic import ValidationError
from sqlalchemy import func, select

from common.constants import ErrorCode, NodeState, ReplicaState, VersionState
from common.errors import VaultError
from common.settings import settings
from health.failure_detector import FailureDetector
from health.heartbeat import HeartbeatPayload, HeartbeatService
from integrity import IntegrityManager
from metadata.database import session_scope
from metadata.models import RepairJob, Replica, StorageNode, Version
from rebalance import RebalanceManager
from repair import RepairManager
from replication.node_client import StorageNodeClient, StorageNodeClientError

from .celery_app import celery_app

_RETRYABLE_ERROR_CODES = frozenset(
    {ErrorCode.NODE_UNAVAILABLE, ErrorCode.INSUFFICIENT_REPLICAS, ErrorCode.STORAGE_FULL}
)


def _run_async(awaitable):
    return asyncio.run(awaitable)


def _retry_task(task: Task, exc: Exception) -> None:
    retryable = isinstance(exc, StorageNodeClientError) or (
        isinstance(exc, VaultError) and exc.code in _RETRYABLE_ERROR_CODES
    )
    if not retryable:
        raise exc
    retry_number = int(getattr(task.request, "retries", 0))
    if retry_number >= task.max_retries:
        raise exc
    countdown = min(300.0, settings.initial_backoff_seconds * (2**retry_number))
    raise task.retry(exc=exc, countdown=countdown)


@celery_app.task(bind=True, name="repair_version", max_retries=max(settings.max_attempts - 1, 0))
def repair_version(self: Task, version_id: str, repair_id: str | None = None) -> dict[str, Any]:
    try:
        with session_scope() as session:
            manager = RepairManager(
                session,
                max_attempts=settings.max_attempts,
                replication_factor=settings.replication_factor,
            )
            if repair_id is not None:
                result = _run_async(manager.run_job(UUID(repair_id)))
                return {
                    "repair_id": str(result.repair_id),
                    "version_id": str(result.version_id),
                    "source_node_id": result.source_node_id,
                    "target_node_id": result.target_node_id,
                    "status": result.status.value,
                    "attempts": result.attempts,
                }
            results = _run_async(
                manager.repair_version_until_healthy(
                    UUID(version_id),
                    replication_factor=settings.replication_factor,
                )
            )
            return {
                "version_id": version_id,
                "repairs": [
                    {
                        "repair_id": str(item.repair_id),
                        "source_node_id": item.source_node_id,
                        "target_node_id": item.target_node_id,
                        "status": item.status.value,
                        "attempts": item.attempts,
                    }
                    for item in results
                ],
            }
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="verify_replica", max_retries=max(settings.max_attempts - 1, 0))
def verify_replica(self: Task, replica_id: str) -> dict[str, Any]:
    try:
        with session_scope() as session:
            result = _run_async(
                IntegrityManager(
                    session,
                    replication_factor=settings.replication_factor,
                    max_attempts=settings.max_attempts,
                ).verify_replica(UUID(replica_id))
            )
            payload = {
                "replica_id": str(result.replica_id),
                "version_id": str(result.version_id),
                "node_id": result.node_id,
                "checked": result.checked,
                "corrupted": result.corrupted,
            }
            if result.repair_id is not None:
                queued = repair_version.apply_async(
                    args=[str(result.version_id)],
                    kwargs={"repair_id": str(result.repair_id)},
                )
                payload["repair_task_id"] = str(queued.id)
            return payload
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="scan_node", max_retries=max(settings.max_attempts - 1, 0))
def scan_node(self: Task, node_id: str) -> dict[str, Any]:
    try:
        with session_scope() as session:
            results = _run_async(
                IntegrityManager(
                    session,
                    replication_factor=settings.replication_factor,
                    max_attempts=settings.max_attempts,
                ).scan_node(node_id)
            )
            task_ids = []
            for result in results:
                if result.repair_id is not None:
                    queued = repair_version.apply_async(
                        args=[str(result.version_id)],
                        kwargs={"repair_id": str(result.repair_id)},
                    )
                    task_ids.append(str(queued.id))
            return {
                "node_id": node_id,
                "checked": sum(result.checked for result in results),
                "corrupted": sum(result.corrupted for result in results),
                "repair_task_ids": task_ids,
            }
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="check_under_replicated_objects", max_retries=max(settings.max_attempts - 1, 0))
def check_under_replicated_objects(self: Task) -> dict[str, Any]:
    try:
        queued = []
        blocked = []
        with session_scope() as session:
            versions = list(
                session.scalars(
                    select(Version).where(Version.state == VersionState.COMMITTED)
                ).all()
            )
            version_ids = [version.version_id for version in versions]
            healthy_counts = {}
            if version_ids:
                healthy_counts = dict(
                    session.execute(
                        select(Replica.version_id, func.count(Replica.replica_id))
                        .where(
                            Replica.version_id.in_(version_ids),
                            Replica.status == ReplicaState.HEALTHY,
                        )
                        .group_by(Replica.version_id)
                    ).all()
                )

            for version in versions:
                healthy_count = int(healthy_counts.get(version.version_id, 0))
                if healthy_count >= settings.replication_factor:
                    continue
                try:
                    job = RepairManager(
                        session,
                        max_attempts=settings.max_attempts,
                        replication_factor=settings.replication_factor,
                    ).schedule_for_version(
                        version.version_id,
                        replication_factor=settings.replication_factor,
                        reason="under-replicated-scan",
                    )
                    if job is None:
                        continue
                    task = repair_version.apply_async(
                        args=[str(version.version_id)],
                        kwargs={"repair_id": str(job.repair_id)},
                    )
                    queued.append({
                        "version_id": str(version.version_id),
                        "repair_id": str(job.repair_id),
                        "task_id": str(task.id),
                    })
                except VaultError as exc:
                    session.rollback()
                    blocked.append({
                        "version_id": str(version.version_id),
                        "error": exc.message,
                    })
        return {"queued": queued, "blocked": blocked}
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="rebalance_node", max_retries=max(settings.max_attempts - 1, 0))
def rebalance_node(self: Task, node_id: str) -> dict[str, Any]:
    try:
        with session_scope() as session:
            results = _run_async(
                RebalanceManager(
                    session,
                    replication_factor=settings.replication_factor,
                    max_attempts=settings.max_attempts,
                ).drain_node(node_id)
            )
            return {
                "node_id": node_id,
                "migrations": [
                    {
                        "rebalance_id": str(item.rebalance_id),
                        "version_id": str(item.version_id),
                        "source_node_id": item.source_node_id,
                        "target_node_id": item.target_node_id,
                        "status": item.status.value,
                        "attempts": item.attempts,
                        "source_removed": item.source_removed,
                    }
                    for item in results
                ],
            }
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="migrate_replica", max_retries=max(settings.max_attempts - 1, 0))
def migrate_replica(
    self: Task,
    replica_id: str,
    target_node_id: str,
    rebalance_id: str | None = None,
) -> dict[str, Any]:
    try:
        with session_scope() as session:
            replica = session.scalar(
                select(Replica).where(Replica.replica_id == UUID(replica_id))
            )
            if replica is None:
                raise VaultError(
                    code=ErrorCode.OBJECT_NOT_FOUND,
                    message=f"Replica '{replica_id}' was not found.",
                    status_code=404,
                )
            manager = RebalanceManager(
                session,
                replication_factor=settings.replication_factor,
                max_attempts=settings.max_attempts,
            )
            result = _run_async(
                manager.run_job(UUID(rebalance_id))
                if rebalance_id is not None
                else manager.migrate_replica(
                    replica.version_id,
                    source_node_id=replica.node_id,
                    target_node_id=target_node_id,
                )
            )
            return {
                "rebalance_id": str(result.rebalance_id),
                "version_id": str(result.version_id),
                "source_node_id": result.source_node_id,
                "target_node_id": result.target_node_id,
                "status": result.status.value,
                "attempts": result.attempts,
                "source_removed": result.source_removed,
            }
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="process_node_health", max_retries=max(settings.max_attempts - 1, 0))
def process_node_health(self: Task) -> dict[str, Any]:
    try:
        transitions = []
        poll_failures = []
        with session_scope() as session:
            heartbeat_service = HeartbeatService(
                session,
                replication_factor=settings.replication_factor,
            )
            nodes = list(
                session.scalars(
                    select(StorageNode).where(
                        StorageNode.status.notin_(
                            (NodeState.DRAINING, NodeState.REMOVED)
                        )
                    ).order_by(StorageNode.node_id)
                ).all()
            )

            async def poll_node(node: StorageNode) -> tuple[StorageNode, HeartbeatPayload | None, str | None]:
                client = StorageNodeClient(
                    node.address,
                    timeout_seconds=settings.storage_request_timeout_seconds,
                )
                try:
                    health, stats = await asyncio.gather(
                        client.health(),
                        client.stats(),
                    )
                    if (
                        health.node_id != node.node_id
                        or health.status.lower() != "healthy"
                    ):
                        raise StorageNodeClientError(
                            "Storage node health response is not healthy or has mismatched identity."
                        )
                    return (
                        node,
                        HeartbeatPayload(
                            node_id=node.node_id,
                            capacity_bytes=stats.capacity_bytes,
                            used_bytes=stats.used_bytes,
                            timestamp=datetime.now(timezone.utc),
                        ),
                        None,
                    )
                except (StorageNodeClientError, ValidationError) as exc:
                    return node, None, str(exc)
                finally:
                    await client.aclose()

            async def poll_nodes() -> list[tuple[StorageNode, HeartbeatPayload | None, str | None]]:
                return list(
                    await asyncio.gather(
                        *(poll_node(node) for node in nodes),
                    )
                )

            for node, payload, error in _run_async(poll_nodes()):
                if error is not None or payload is None:
                    poll_failures.append({"node_id": node.node_id, "error": error or "heartbeat payload unavailable"})
                    continue
                result = (
                    _run_async(heartbeat_service.ingest_and_recover(payload))
                    if node.status is NodeState.UNAVAILABLE
                    else heartbeat_service.ingest(payload)
                )
                transitions.append({
                    "node_id": node.node_id,
                    "status": result.status.value,
                    "accepted": result.accepted,
                })

            detector = FailureDetector(
                session,
                suspect_after_seconds=settings.suspect_after_seconds,
                unavailable_after_seconds=settings.unavailable_after_seconds,
                replication_factor=settings.replication_factor,
            )
            for transition in detector.scan():
                transitions.append({
                    "node_id": transition.node_id,
                    "previous": transition.previous.value,
                    "current": transition.current.value,
                    "repair_ids": [str(item) for item in transition.repair_ids],
                })
                for repair_id in transition.repair_ids:
                    version_id = session.scalar(
                        select(RepairJob.version_id).where(
                            RepairJob.repair_id == repair_id
                        )
                    )
                    if version_id is not None:
                        repair_version.apply_async(
                            args=[str(version_id)],
                            kwargs={"repair_id": str(repair_id)},
                        )
        return {"transitions": transitions, "poll_failures": poll_failures}
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="run_integrity_check", max_retries=max(settings.max_attempts - 1, 0))
def run_integrity_check(self: Task, integrity_id: str) -> dict[str, Any]:
    try:
        with session_scope() as session:
            result = _run_async(
                IntegrityManager(
                    session,
                    replication_factor=settings.replication_factor,
                    max_attempts=settings.max_attempts,
                ).run_job(UUID(integrity_id))
            )
            repair_task_ids = []
            for item in result.results:
                if item.repair_id is not None:
                    queued = repair_version.apply_async(
                        args=[str(item.version_id)],
                        kwargs={"repair_id": str(item.repair_id)},
                    )
                    repair_task_ids.append(str(queued.id))
            return {
                "integrity_id": str(result.integrity_id),
                "status": result.status.value,
                "attempts": result.attempts,
                "checked_count": result.checked_count,
                "corrupted_count": result.corrupted_count,
                "repair_task_ids": repair_task_ids,
            }
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")


@celery_app.task(bind=True, name="scan_all_integrity", max_retries=max(settings.max_attempts - 1, 0))
def scan_all_integrity(self: Task) -> dict[str, Any]:
    try:
        with session_scope() as session:
            job = IntegrityManager(
                session,
                replication_factor=settings.replication_factor,
                max_attempts=settings.max_attempts,
            ).create_job()
            queued = run_integrity_check.apply_async(args=[str(job.integrity_id)])
            return {"integrity_id": str(job.integrity_id), "task_id": str(queued.id)}
    except Exception as exc:
        _retry_task(self, exc)
        raise AssertionError("unreachable")

"""Celery application and periodic schedule for Vault background work."""
from __future__ import annotations

from celery import Celery

from common.settings import settings

celery_app = Celery(
    "vault",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=("worker.tasks",),
)

celery_app.conf.update(
    task_default_queue="vault.control",
    task_serializer="json",
    result_serializer="json",
    accept_content=("json",),
    enable_utc=True,
    timezone="UTC",
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=settings.max_concurrent_jobs,
    broker_connection_retry_on_startup=True,
    task_routes={
        "repair_version": {"queue": "vault.repair"},
        "verify_replica": {"queue": "vault.integrity"},
        "scan_node": {"queue": "vault.integrity"},
        "check_under_replicated_objects": {"queue": "vault.repair"},
        "rebalance_node": {"queue": "vault.rebalance"},
        "migrate_replica": {"queue": "vault.rebalance"},
        "process_node_health": {"queue": "vault.health"},
        "run_integrity_check": {"queue": "vault.integrity"},
        "scan_all_integrity": {"queue": "vault.integrity"},
    },
    beat_schedule={
        "process-node-health": {
            "task": "process_node_health",
            "schedule": settings.heartbeat_interval_seconds,
        },
        "scan-under-replicated-objects": {
            "task": "check_under_replicated_objects",
            "schedule": settings.under_replicated_scan_interval_seconds,
        },
        "scan-all-integrity": {
            "task": "scan_all_integrity",
            "schedule": settings.integrity_scan_interval_seconds,
        },
    },
)

__all__ = ["celery_app"]

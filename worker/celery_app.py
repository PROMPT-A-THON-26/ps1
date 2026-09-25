"""Celery application and periodic background-job schedule for Vault."""

from __future__ import annotations

from celery import Celery

from common.config import get_settings

settings = get_settings()

celery_app = Celery(
    "vault",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["worker.tasks"],
)

celery_app.conf.update(
    task_default_queue="control",
    task_routes={
        "repair_version": {"queue": "repair"},
        "run_repair_job": {"queue": "repair"},
        "verify_replica": {"queue": "repair"},
        "scan_node": {"queue": "repair"},
        "scan_version": {"queue": "repair"},
        "check_under_replicated_objects": {"queue": "repair"},
        "rebalance_node": {"queue": "rebalance"},
        "run_rebalance_job": {"queue": "rebalance"},
        "migrate_replica": {"queue": "rebalance"},
        "rebalance_draining_nodes": {"queue": "rebalance"},
        "process_node_health": {"queue": "health"},
        "scan_all_nodes": {"queue": "repair"},
    },
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_concurrency=settings.max_concurrent_jobs,
    task_track_started=True,
    result_expires=86400,
    beat_schedule={
        "process-node-health": {
            "task": "process_node_health",
            "schedule": settings.heartbeat_interval_seconds,
        },
        "check-under-replicated-objects": {
            "task": "check_under_replicated_objects",
            "schedule": settings.under_replication_scan_interval_seconds,
        },
        "scan-all-nodes-for-integrity": {
            "task": "scan_all_nodes",
            "schedule": settings.integrity_scan_interval_seconds,
        },
        "rebalance-draining-nodes": {
            "task": "rebalance_draining_nodes",
            "schedule": settings.rebalance_scan_interval_seconds,
        },
    },
)

__all__ = ["celery_app"]

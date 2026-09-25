from __future__ import annotations

from common.config import get_settings
from worker.celery_app import celery_app


EXPECTED = {
    "repair_version",
    "verify_replica",
    "scan_node",
    "scan_version",
    "check_under_replicated_objects",
    "rebalance_node",
    "migrate_replica",
    "process_node_health",
}


def test_canonical_background_task_names_are_registered():
    assert EXPECTED.issubset(set(celery_app.tasks))


def test_worker_concurrency_and_retry_policy_are_configuration_driven():
    settings = get_settings()
    assert celery_app.conf.worker_concurrency == settings.max_concurrent_jobs
    assert celery_app.conf.worker_prefetch_multiplier == 1
    assert celery_app.conf.task_acks_late is True

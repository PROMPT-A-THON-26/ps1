from common.settings import settings
from worker import tasks


def test_canonical_worker_task_names():
    assert {
        tasks.repair_version.name,
        tasks.verify_replica.name,
        tasks.scan_node.name,
        tasks.check_under_replicated_objects.name,
        tasks.rebalance_node.name,
        tasks.migrate_replica.name,
        tasks.process_node_health.name,
    } == {
        "repair_version",
        "verify_replica",
        "scan_node",
        "check_under_replicated_objects",
        "rebalance_node",
        "migrate_replica",
        "process_node_health",
    }


def test_worker_settings_are_valid():
    assert settings.max_attempts >= 1
    assert settings.max_concurrent_jobs >= 1
    assert settings.replication_factor >= settings.write_quorum
    assert settings.replication_factor >= settings.read_quorum
    assert settings.initial_backoff_seconds >= 0

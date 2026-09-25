from __future__ import annotations

import pytest

from worker.main import WorkerRuntimeConfig, parse_storage_nodes


def test_parse_storage_nodes_accepts_multiple_nodes():
    nodes = parse_storage_nodes(
        "node-01=http://node-01:9001,node-02=https://node-02:9443"
    )
    assert nodes == {
        "node-01": "http://node-01:9001",
        "node-02": "https://node-02:9443",
    }


@pytest.mark.parametrize(
    "value",
    [
        "node-01",
        "node-01=ftp://node-01:9001",
        "node-01=http://node-01:9001,node-01=http://other:9001",
        "=http://node-01:9001",
    ],
)
def test_parse_storage_nodes_rejects_invalid_entries(value):
    with pytest.raises(ValueError):
        parse_storage_nodes(value)


def test_worker_runtime_defaults_validate_replication_policy():
    config = WorkerRuntimeConfig(
        database_url="sqlite:///./vault.db",
        storage_nodes={"node-01": "http://node-01:9001"},
    )
    assert config.replication_factor == 3
    assert config.write_quorum == 2
    assert config.read_quorum == 1
    assert config.rebalance_high_watermark == 0.80
    assert config.rebalance_low_watermark == 0.60

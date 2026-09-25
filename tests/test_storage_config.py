import pytest

from storage.config import ConfigurationError, StorageNodeConfig


def test_config_rejects_invalid_port(monkeypatch):
    monkeypatch.setenv("VAULT_NODE_PORT", "70000")
    with pytest.raises(ConfigurationError, match="between 1 and 65535"):
        StorageNodeConfig.from_env()


def test_config_rejects_state_database_inside_data_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("VAULT_NODE_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv(
        "VAULT_NODE_SQLITE_PATH",
        str(tmp_path / "data" / "state.sqlite3"),
    )
    with pytest.raises(ConfigurationError, match="outside VAULT_NODE_DATA_DIR"):
        StorageNodeConfig.from_env()


def test_config_accepts_state_database_outside_data_dir(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    state_path = tmp_path / "state" / "node.sqlite3"
    monkeypatch.setenv("VAULT_NODE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("VAULT_NODE_SQLITE_PATH", str(state_path))

    config = StorageNodeConfig.from_env()

    assert config.data_dir == data_dir
    assert config.sqlite_path == state_path


def test_config_rejects_directory_state_path(monkeypatch, tmp_path):
    data_dir = tmp_path / "data"
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    monkeypatch.setenv("VAULT_NODE_DATA_DIR", str(data_dir))
    monkeypatch.setenv("VAULT_NODE_SQLITE_PATH", str(state_dir))

    with pytest.raises(ConfigurationError, match="must point to a file"):
        StorageNodeConfig.from_env()

import os
from pathlib import Path

from database.settings import DatabaseSettings


def test_database_settings_use_explicit_connection_overrides(monkeypatch, tmp_path):
    sqlite_path = tmp_path / "node.sqlite3"
    monkeypatch.setenv("VAULT_POSTGRES_DSN", "postgresql://user:pass@db.example:5432/vault")
    monkeypatch.setenv("VAULT_MONGO_URI", "mongodb://user:pass@mongo.example:27017/?authSource=admin")
    monkeypatch.setenv("VAULT_MONGO_DATABASE", "events")
    monkeypatch.setenv("VAULT_SQLITE_PATH", str(sqlite_path))

    settings = DatabaseSettings.from_env()

    assert settings.postgres_dsn == "postgresql://user:pass@db.example:5432/vault"
    assert settings.mongo_uri.startswith("mongodb://user:pass@mongo.example:27017/")
    assert settings.mongo_database == "events"
    assert settings.sqlite_path == sqlite_path


def test_database_settings_build_from_compose_style_environment(monkeypatch):
    for name in (
        "VAULT_POSTGRES_DSN",
        "VAULT_MONGO_URI",
        "VAULT_MONGO_DATABASE",
        "VAULT_SQLITE_PATH",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "vault")
    monkeypatch.setenv("POSTGRES_USER", "vault-user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss")
    monkeypatch.setenv("MONGO_HOST", "mongodb")
    monkeypatch.setenv("MONGO_PORT", "27017")
    monkeypatch.setenv("MONGO_INITDB_ROOT_USERNAME", "mongo-user")
    monkeypatch.setenv("MONGO_INITDB_ROOT_PASSWORD", "m/p@ss")
    monkeypatch.setenv("MONGO_DB", "vault-events")

    settings = DatabaseSettings.from_env()

    assert settings.postgres_dsn == "postgresql://vault-user:p%40ss@postgres:5432/vault"
    assert settings.mongo_uri == (
        "mongodb://mongo-user:m%2Fp%40ss@mongodb:27017/?authSource=admin"
    )
    assert settings.mongo_database == "vault-events"
    assert settings.sqlite_path == Path("./data/vault-state/node_state.sqlite3")


def test_database_settings_do_not_require_external_connections():
    settings = DatabaseSettings.from_env()
    assert settings.postgres_dsn
    assert settings.mongo_uri
    assert settings.mongo_database
    assert settings.sqlite_path

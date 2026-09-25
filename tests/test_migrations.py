from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def _config(database_url: str) -> Config:
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def test_initial_metadata_migration_up_and_down(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'migration.db'}"
    config = _config(database_url)

    command.upgrade(config, "head")

    engine = create_engine(database_url)
    tables = set(inspect(engine).get_table_names())
    assert {
        "objects",
        "versions",
        "replicas",
        "storage_nodes",
        "repair_jobs",
        "rebalance_jobs",
        "alembic_version",
    }.issubset(tables)
    engine.dispose()

    command.downgrade(config, "base")

    engine = create_engine(database_url)
    assert not set(inspect(engine).get_table_names()) & {
        "objects",
        "versions",
        "replicas",
        "storage_nodes",
        "repair_jobs",
        "rebalance_jobs",
    }
    engine.dispose()

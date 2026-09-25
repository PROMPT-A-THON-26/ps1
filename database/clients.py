from __future__ import annotations

import sqlite3
from typing import Any

from .settings import DatabaseSettings


def connect_postgres(settings: DatabaseSettings | None = None) -> Any:
    """Create a PostgreSQL connection on demand."""
    import psycopg

    cfg = settings or DatabaseSettings.from_env()
    return psycopg.connect(cfg.postgres_dsn)


def connect_mongodb(settings: DatabaseSettings | None = None) -> tuple[Any, Any]:
    """Create a MongoDB client and return (client, database) on demand."""
    from pymongo import MongoClient

    cfg = settings or DatabaseSettings.from_env()
    client = MongoClient(cfg.mongo_uri)
    return client, client[cfg.mongo_database]


def connect_sqlite(settings: DatabaseSettings | None = None) -> sqlite3.Connection:
    """Create the local SQLite connection used by a process."""
    cfg = settings or DatabaseSettings.from_env()
    cfg.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(cfg.sqlite_path, timeout=30, check_same_thread=False)

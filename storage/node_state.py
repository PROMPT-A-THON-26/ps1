from __future__ import annotations

import sqlite3
from pathlib import Path
from threading import Lock


class SQLiteNodeStateStore:
    """Small durable SQLite store for local storage-node state."""

    def __init__(self, db_path: Path) -> None:
        self.path = Path(db_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._connection = sqlite3.connect(
            self.path,
            timeout=30,
            check_same_thread=False,
        )
        with self._lock:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS node_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            self._connection.commit()

    def get_state(self) -> str | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT value FROM node_state WHERE key = ?",
                ("lifecycle_state",),
            ).fetchone()
        return None if row is None else str(row[0])

    def set_state(self, state: str) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT INTO node_state(key, value)
                VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                ("lifecycle_state", state),
            )
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

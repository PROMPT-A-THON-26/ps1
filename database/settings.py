from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    postgres_dsn: str
    mongo_uri: str
    mongo_database: str
    sqlite_path: Path

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        return cls(
            postgres_dsn=os.getenv(
                "VAULT_POSTGRES_DSN",
                "postgresql://vault@localhost:5432/vault",
            ),
            mongo_uri=os.getenv(
                "VAULT_MONGO_URI",
                "mongodb://localhost:27017",
            ),
            mongo_database=os.getenv("VAULT_MONGO_DATABASE", "vault"),
            sqlite_path=Path(
                os.getenv(
                    "VAULT_SQLITE_PATH",
                    "./data/vault/vault.sqlite3",
                )
            ).expanduser(),
        )

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    postgres_dsn: str
    mongo_uri: str
    mongo_database: str
    sqlite_path: Path

    @classmethod
    def from_env(cls) -> "DatabaseSettings":
        postgres_dsn = os.getenv("VAULT_POSTGRES_DSN")
        if not postgres_dsn:
            postgres_host = os.getenv("POSTGRES_HOST", "localhost")
            postgres_port = os.getenv("POSTGRES_PORT", "5432")
            postgres_db = os.getenv("POSTGRES_DB", "vault")
            postgres_user = os.getenv("POSTGRES_USER", "vault")
            postgres_password = os.getenv("POSTGRES_PASSWORD", "")
            auth = quote(postgres_user, safe="")
            if postgres_password:
                auth += ":" + quote(postgres_password, safe="")
            postgres_dsn = (
                f"postgresql://{auth}@{postgres_host}:{postgres_port}/{postgres_db}"
            )

        mongo_uri = os.getenv("VAULT_MONGO_URI")
        if not mongo_uri:
            mongo_host = os.getenv("MONGO_HOST", "localhost")
            mongo_port = os.getenv("MONGO_PORT", "27017")
            mongo_user = os.getenv("MONGO_INITDB_ROOT_USERNAME", "")
            mongo_password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "")
            if mongo_user and mongo_password:
                mongo_uri = (
                    "mongodb://"
                    f"{quote(mongo_user, safe='')}:{quote(mongo_password, safe='')}"
                    f"@{mongo_host}:{mongo_port}/?authSource=admin"
                )
            else:
                mongo_uri = f"mongodb://{mongo_host}:{mongo_port}"

        return cls(
            postgres_dsn=postgres_dsn,
            mongo_uri=mongo_uri,
            mongo_database=os.getenv(
                "VAULT_MONGO_DATABASE",
                os.getenv("MONGO_DB", "vault"),
            ),
            sqlite_path=Path(
                os.getenv(
                    "VAULT_SQLITE_PATH",
                    "./data/vault-state/node_state.sqlite3",
                )
            ).expanduser(),
        )

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigurationError(ValueError):
    """Raised when a storage-node configuration value is invalid."""


@dataclass(frozen=True, slots=True)
class StorageNodeConfig:
    node_id: str
    host: str
    port: int
    data_dir: Path
    sqlite_path: Path
    capacity_bytes: int
    chunk_size_bytes: int

    @classmethod
    def from_env(cls) -> "StorageNodeConfig":
        node_id = os.getenv("VAULT_NODE_ID", "node-01").strip()
        host = os.getenv("VAULT_NODE_HOST", "0.0.0.0").strip()
        port = _positive_int(os.getenv("VAULT_NODE_PORT", "9001"), "VAULT_NODE_PORT")
        capacity_bytes = _positive_int(
            os.getenv("VAULT_NODE_CAPACITY_BYTES", str(100 * 1024**3)),
            "VAULT_NODE_CAPACITY_BYTES",
        )
        chunk_size_bytes = _positive_int(
            os.getenv("VAULT_NODE_CHUNK_SIZE_BYTES", str(16 * 1024**2)),
            "VAULT_NODE_CHUNK_SIZE_BYTES",
        )
        data_dir = Path(
            os.getenv("VAULT_NODE_DATA_DIR", "./data/vault")
        ).expanduser()
        sqlite_path = Path(
            os.getenv(
                "VAULT_NODE_SQLITE_PATH",
                str(data_dir.parent / "vault-state" / "node_state.sqlite3"),
            )
        ).expanduser()

        if not node_id:
            raise ConfigurationError("VAULT_NODE_ID must not be empty")
        if not host:
            raise ConfigurationError("VAULT_NODE_HOST must not be empty")

        return cls(
            node_id=node_id,
            host=host,
            port=port,
            data_dir=data_dir,
            sqlite_path=sqlite_path,
            capacity_bytes=capacity_bytes,
            chunk_size_bytes=chunk_size_bytes,
        )


def _positive_int(raw: str, name: str) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value

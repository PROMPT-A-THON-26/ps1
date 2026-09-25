"""Central environment-backed Vault configuration."""

from __future__ import annotations

from dataclasses import dataclass
import os

from common.constants import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_MAX_PARALLEL_REPAIRS,
    DEFAULT_READ_QUORUM,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_SUSPECT_AFTER_SECONDS,
    DEFAULT_UNAVAILABLE_AFTER_SECONDS,
    DEFAULT_WRITE_QUORUM,
)


def _read(name: str, legacy: str | None = None) -> str | None:
    value = os.getenv(name)
    return value if value is not None else (os.getenv(legacy) if legacy else None)


def _int(name: str, default: int, *, minimum: int = 0, legacy: str | None = None) -> int:
    raw = _read(name, legacy)
    value = default if raw is None or not raw.strip() else int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _float(name: str, default: float, *, minimum: float = 0.0, legacy: str | None = None) -> float:
    raw = _read(name, legacy)
    value = default if raw is None or not raw.strip() else float(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class VaultSettings:
    database_url: str
    replication_factor: int
    write_quorum: int
    read_quorum: int
    heartbeat_interval_seconds: float
    suspect_after_seconds: float
    unavailable_after_seconds: float
    max_concurrent_jobs: int
    max_attempts: int
    initial_backoff_seconds: float
    storage_request_timeout_seconds: float
    celery_broker_url: str
    celery_result_backend: str
    integrity_scan_interval_seconds: float
    under_replication_scan_interval_seconds: float
    rebalance_scan_interval_seconds: float

    @classmethod
    def from_env(cls) -> "VaultSettings":
        factor = _int("VAULT_REPLICATION_FACTOR", DEFAULT_REPLICATION_FACTOR, minimum=1, legacy="REPLICATION_FACTOR")
        write_quorum = _int("VAULT_WRITE_QUORUM", DEFAULT_WRITE_QUORUM, minimum=1, legacy="WRITE_QUORUM")
        read_quorum = _int("VAULT_READ_QUORUM", DEFAULT_READ_QUORUM, minimum=1, legacy="READ_QUORUM")
        if write_quorum > factor:
            raise ValueError("VAULT_WRITE_QUORUM must not exceed VAULT_REPLICATION_FACTOR")
        if read_quorum > factor:
            raise ValueError("VAULT_READ_QUORUM must not exceed VAULT_REPLICATION_FACTOR")

        suspect = _float(
            "VAULT_SUSPECT_AFTER_SECONDS",
            DEFAULT_SUSPECT_AFTER_SECONDS,
            minimum=0.001,
            legacy="SUSPECT_AFTER_SECONDS",
        )
        unavailable = _float(
            "VAULT_UNAVAILABLE_AFTER_SECONDS",
            DEFAULT_UNAVAILABLE_AFTER_SECONDS,
            minimum=suspect + 0.001,
            legacy="UNAVAILABLE_AFTER_SECONDS",
        )

        return cls(
            database_url=os.getenv("DATABASE_URL", "sqlite:///./vault.db"),
            replication_factor=factor,
            write_quorum=write_quorum,
            read_quorum=read_quorum,
            heartbeat_interval_seconds=_float(
                "VAULT_HEARTBEAT_INTERVAL_SECONDS",
                DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                minimum=0.001,
                legacy="HEARTBEAT_INTERVAL_SECONDS",
            ),
            suspect_after_seconds=suspect,
            unavailable_after_seconds=unavailable,
            max_concurrent_jobs=_int(
                "VAULT_MAX_CONCURRENT_JOBS",
                DEFAULT_MAX_PARALLEL_REPAIRS,
                minimum=1,
                legacy="MAX_PARALLEL_REPAIRS",
            ),
            max_attempts=_int("VAULT_MAX_ATTEMPTS", 5, minimum=1),
            initial_backoff_seconds=_float(
                "VAULT_INITIAL_BACKOFF_SECONDS",
                2.0,
                minimum=0.0,
            ),
            storage_request_timeout_seconds=_float(
                "VAULT_STORAGE_REQUEST_TIMEOUT_SECONDS",
                30.0,
                minimum=0.001,
            ),
            celery_broker_url=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
            celery_result_backend=os.getenv(
                "CELERY_RESULT_BACKEND",
                os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
            ),
            integrity_scan_interval_seconds=_float(
                "VAULT_INTEGRITY_SCAN_INTERVAL_SECONDS",
                3600.0,
                minimum=1.0,
            ),
            under_replication_scan_interval_seconds=_float(
                "VAULT_UNDER_REPLICATION_SCAN_INTERVAL_SECONDS",
                30.0,
                minimum=1.0,
            ),
            rebalance_scan_interval_seconds=_float(
                "VAULT_REBALANCE_SCAN_INTERVAL_SECONDS",
                30.0,
                minimum=1.0,
            ),
        )


settings = VaultSettings.from_env()


def get_settings() -> VaultSettings:
    """Return the process-wide immutable Vault settings."""
    return settings

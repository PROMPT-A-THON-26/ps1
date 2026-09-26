"""Centralized runtime settings for the Vault control plane."""
from __future__ import annotations

import os
from dataclasses import dataclass

from .constants import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_INITIAL_BACKOFF_SECONDS,
    DEFAULT_MAX_PARALLEL_REPAIRS,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_READ_QUORUM,
    DEFAULT_STORAGE_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_SUSPECT_AFTER_SECONDS,
    DEFAULT_UNAVAILABLE_AFTER_SECONDS,
    DEFAULT_UNDER_REPLICATED_SCAN_INTERVAL_SECONDS,
    DEFAULT_INTEGRITY_SCAN_INTERVAL_SECONDS,
    DEFAULT_WRITE_QUORUM,
)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else float(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


@dataclass(frozen=True, slots=True)
class VaultSettings:
    database_url: str = "sqlite:///./vault.db"
    replication_factor: int = DEFAULT_REPLICATION_FACTOR
    write_quorum: int = DEFAULT_WRITE_QUORUM
    read_quorum: int = DEFAULT_READ_QUORUM
    heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    suspect_after_seconds: float = DEFAULT_SUSPECT_AFTER_SECONDS
    unavailable_after_seconds: float = DEFAULT_UNAVAILABLE_AFTER_SECONDS
    max_concurrent_jobs: int = DEFAULT_MAX_PARALLEL_REPAIRS
    max_attempts: int = 5
    initial_backoff_seconds: float = DEFAULT_INITIAL_BACKOFF_SECONDS
    storage_request_timeout_seconds: float = DEFAULT_STORAGE_REQUEST_TIMEOUT_SECONDS
    integrity_scan_interval_seconds: float = DEFAULT_INTEGRITY_SCAN_INTERVAL_SECONDS
    under_replicated_scan_interval_seconds: float = DEFAULT_UNDER_REPLICATED_SCAN_INTERVAL_SECONDS
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    max_upload_bytes: int = 100 * 1024 * 1024

    @classmethod
    def from_env(cls) -> "VaultSettings":
        defaults = cls()
        factor = _env_int("REPLICATION_FACTOR", defaults.replication_factor, minimum=1)
        write_quorum = _env_int("WRITE_QUORUM", defaults.write_quorum, minimum=1)
        read_quorum = _env_int("READ_QUORUM", defaults.read_quorum, minimum=1)
        if write_quorum > factor or read_quorum > factor:
            raise ValueError(
                "WRITE_QUORUM and READ_QUORUM must not exceed REPLICATION_FACTOR"
            )

        heartbeat = _env_float(
            "HEARTBEAT_INTERVAL_SECONDS",
            defaults.heartbeat_interval_seconds,
            minimum=0.1,
        )
        suspect = _env_float(
            "SUSPECT_AFTER_SECONDS",
            defaults.suspect_after_seconds,
            minimum=heartbeat,
        )
        unavailable = _env_float(
            "UNAVAILABLE_AFTER_SECONDS",
            defaults.unavailable_after_seconds,
            minimum=suspect,
        )
        broker = os.getenv("CELERY_BROKER_URL", defaults.celery_broker_url).strip()
        backend = os.getenv("CELERY_RESULT_BACKEND", defaults.celery_result_backend).strip()
        if not broker or not backend:
            raise ValueError("CELERY_BROKER_URL and CELERY_RESULT_BACKEND must not be empty")

        return cls(
            database_url=os.getenv("DATABASE_URL", defaults.database_url),
            replication_factor=factor,
            write_quorum=write_quorum,
            read_quorum=read_quorum,
            heartbeat_interval_seconds=heartbeat,
            suspect_after_seconds=suspect,
            unavailable_after_seconds=unavailable,
            max_concurrent_jobs=_env_int(
                "MAX_PARALLEL_REPAIRS", defaults.max_concurrent_jobs, minimum=1
            ),
            max_attempts=_env_int("REPAIR_MAX_ATTEMPTS", defaults.max_attempts, minimum=1),
            initial_backoff_seconds=_env_float(
                "REPAIR_INITIAL_BACKOFF_SECONDS",
                defaults.initial_backoff_seconds,
                minimum=0.0,
            ),
            storage_request_timeout_seconds=_env_float(
                "STORAGE_REQUEST_TIMEOUT_SECONDS",
                defaults.storage_request_timeout_seconds,
                minimum=0.1,
            ),
            integrity_scan_interval_seconds=_env_float(
                "INTEGRITY_SCAN_INTERVAL_SECONDS",
                defaults.integrity_scan_interval_seconds,
                minimum=1.0,
            ),
            under_replicated_scan_interval_seconds=_env_float(
                "UNDER_REPLICATED_SCAN_INTERVAL_SECONDS",
                defaults.under_replicated_scan_interval_seconds,
                minimum=1.0,
            ),
            celery_broker_url=broker,
            celery_result_backend=backend,
            max_upload_bytes=_env_int("MAX_UPLOAD_BYTES", defaults.max_upload_bytes, minimum=1),
        )


settings = VaultSettings.from_env()

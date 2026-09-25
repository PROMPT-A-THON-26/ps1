"""Executable reliability-worker runtime for local and Docker deployments."""
from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Mapping

from sqlalchemy.orm import sessionmaker

from health.failure_detector import FailureDetector, FailureDetectorConfig
from integrity.scanner import IntegrityScanner
from metadata.database import build_engine, create_schema
from metadata.manager import MetadataManager
from rebalance.rebalancer import RebalancePolicy, Rebalancer
from repair.worker import RepairWorker, RepairWorkerConfig
from replication.coordinator import DistributedWriteCoordinator
from replication.node_client import RetryPolicy, StorageNodeClient, StorageNodeClientConfig
from worker.reliability_service import ReliabilityService, ReliabilityServiceConfig


@dataclass(frozen=True, slots=True)
class WorkerRuntimeConfig:
    database_url: str
    storage_nodes: Mapping[str, str]
    replication_factor: int = 3
    write_quorum: int = 2
    read_quorum: int = 1
    reliability_interval_seconds: float = 5.0
    suspect_after_seconds: float = 15.0
    unavailable_after_seconds: float = 30.0
    repair_batch_size: int = 16
    integrity_batch_size: int | None = None
    rebalance_high_watermark: float = 0.80
    rebalance_low_watermark: float = 0.60
    rebalance_max_moves: int = 8

    @classmethod
    def from_env(cls) -> "WorkerRuntimeConfig":
        nodes = parse_storage_nodes(os.getenv("VAULT_STORAGE_NODES", ""))
        if not nodes:
            raise ValueError(
                "VAULT_STORAGE_NODES must contain at least one node in "
                "'node-id=http://host:port' form"
            )

        rf = _positive_int("REPLICATION_FACTOR", 3)
        w = _positive_int("WRITE_QUORUM", 2)
        r = _positive_int("READ_QUORUM", 1)
        if not 1 <= w <= rf:
            raise ValueError("WRITE_QUORUM must be between 1 and REPLICATION_FACTOR")
        if not 1 <= r <= rf:
            raise ValueError("READ_QUORUM must be between 1 and REPLICATION_FACTOR")

        integrity_value = os.getenv("INTEGRITY_BATCH_SIZE", "").strip()
        integrity_batch = int(integrity_value) if integrity_value else None
        if integrity_batch is not None and integrity_batch < 1:
            raise ValueError("INTEGRITY_BATCH_SIZE must be at least 1")

        return cls(
            database_url=os.getenv(
                "DATABASE_URL",
                "sqlite:///./vault.db",
            ),
            storage_nodes=nodes,
            replication_factor=rf,
            write_quorum=w,
            read_quorum=r,
            reliability_interval_seconds=_positive_float(
                "RELIABILITY_INTERVAL_SECONDS", 5.0
            ),
            suspect_after_seconds=_positive_float(
                "SUSPECT_AFTER_SECONDS", 15.0
            ),
            unavailable_after_seconds=_positive_float(
                "UNAVAILABLE_AFTER_SECONDS", 30.0
            ),
            repair_batch_size=_positive_int("REPAIR_BATCH_SIZE", 16),
            integrity_batch_size=integrity_batch,
            rebalance_high_watermark=float(
                os.getenv("REBALANCE_HIGH_WATERMARK", "0.80")
            ),
            rebalance_low_watermark=float(
                os.getenv("REBALANCE_LOW_WATERMARK", "0.60")
            ),
            rebalance_max_moves=_positive_int("REBALANCE_MAX_MOVES_PER_SCAN", 8),
        )


def parse_storage_nodes(value: str) -> dict[str, str]:
    """Parse comma-separated node-id=absolute-http-url pairs."""
    nodes: dict[str, str] = {}
    if not isinstance(value, str):
        raise ValueError("VAULT_STORAGE_NODES must be a string")

    for raw_entry in value.split(","):
        entry = raw_entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(
                "Each VAULT_STORAGE_NODES entry must use node-id=http://host:port"
            )
        node_id, address = (part.strip() for part in entry.split("=", 1))
        if not node_id or not address:
            raise ValueError("VAULT_STORAGE_NODES contains an empty node id/address")
        if node_id in nodes:
            raise ValueError(f"Duplicate storage node id: {node_id}")
        if not (address.startswith("http://") or address.startswith("https://")):
            raise ValueError(f"Storage node address must be absolute HTTP(S): {address}")
        nodes[node_id] = address.rstrip("/")

    return nodes


def _positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be at least 1")
    return value


def _positive_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


async def run_runtime(config: WorkerRuntimeConfig) -> None:
    """Start the full detection -> integrity -> repair -> rebalance loop."""
    engine = build_engine(config.database_url)
    create_schema(engine)

    session_factory = sessionmaker(bind=engine, expire_on_commit=False)
    with session_factory() as session:
        manager = MetadataManager(session)

        async with AsyncExitStack() as stack:
            clients: dict[str, StorageNodeClient] = {}
            for node_id, address in config.storage_nodes.items():
                client = StorageNodeClient(
                    StorageNodeClientConfig(
                        address,
                        retry_policy=RetryPolicy(
                            max_attempts=3,
                            base_delay_seconds=0.2,
                            max_delay_seconds=2.0,
                            jitter_ratio=0.0,
                        ),
                    )
                )
                await stack.enter_async_context(client)
                clients[node_id] = client

                manager.register_node(
                    node_id=node_id,
                    address=address,
                    capacity_bytes=0,
                )

            coordinator = DistributedWriteCoordinator(
                session,
                clients,
                replication_factor=config.replication_factor,
                write_quorum=config.write_quorum,
                read_quorum=config.read_quorum,
            )
            detector = FailureDetector(
                session,
                clients,
                config=FailureDetectorConfig(
                    interval_seconds=config.reliability_interval_seconds,
                    suspect_after_seconds=config.suspect_after_seconds,
                    unavailable_after_seconds=config.unavailable_after_seconds,
                ),
            )
            scanner = IntegrityScanner(
                session,
                clients,
                max_versions_per_scan=config.integrity_batch_size,
            )
            repair_worker = RepairWorker(
                session,
                coordinator,
                config=RepairWorkerConfig(
                    interval_seconds=config.reliability_interval_seconds,
                    batch_size=config.repair_batch_size,
                ),
            )
            rebalancer = Rebalancer(
                session,
                clients,
                replication_factor=config.replication_factor,
                policy=RebalancePolicy(
                    high_watermark=config.rebalance_high_watermark,
                    low_watermark=config.rebalance_low_watermark,
                    max_moves_per_scan=config.rebalance_max_moves,
                ),
            )

            service = ReliabilityService(
                detector,
                repair_worker,
                integrity_scanner=scanner,
                rebalancer=rebalancer,
                config=ReliabilityServiceConfig(
                    interval_seconds=config.reliability_interval_seconds,
                ),
            )

            stop_event = asyncio.Event()
            await service.run_forever(stop_event)

    engine.dispose()


def main() -> None:
    config = WorkerRuntimeConfig.from_env()
    asyncio.run(run_runtime(config))


if __name__ == "__main__":
    main()

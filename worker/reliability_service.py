"""Single-process reliability loop for detection, integrity, repair, and rebalancing."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from health.failure_detector import FailureDetector, ProbeResult
from repair.worker import RepairRunResult, RepairWorker
from integrity.scanner import IntegrityScanResult, IntegrityScanner


@dataclass(frozen=True, slots=True)
class ReliabilityServiceConfig:
    """Scheduling policy for the combined reliability loop."""

    interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")


class ReliabilityService:
    """Run detection, integrity classification, repair, then rebalancing.

    The ordering is intentional:
    detection establishes liveness, integrity classification identifies
    corruption/unavailability, repair restores the durability floor, and only
    then may rebalancing move healthy replicas.
    """

    def __init__(
        self,
        detector: FailureDetector,
        repair_worker: RepairWorker,
        *,
        integrity_scanner: IntegrityScanner | None = None,
        rebalancer: Any | None = None,
        config: ReliabilityServiceConfig | None = None,
    ) -> None:
        self.detector = detector
        self.integrity_scanner = integrity_scanner
        self.repair_worker = repair_worker
        self.rebalancer = rebalancer
        self.config = config or ReliabilityServiceConfig()

    async def run_once(
        self,
    ) -> tuple[tuple[ProbeResult, ...], IntegrityScanResult | None, RepairRunResult]:
        probes = await self.detector.scan_once()
        integrity_result = (
            await self.integrity_scanner.scan_once()
            if self.integrity_scanner is not None
            else None
        )
        repair_result = await self.repair_worker.run_once()
        if self.rebalancer is not None:
            await self.rebalancer.run_once()
        return probes, integrity_result, repair_result

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Run the reliability pipeline until stop_event is set."""
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.config.interval_seconds,
                )
            except asyncio.TimeoutError:
                continue


__all__ = ["ReliabilityService", "ReliabilityServiceConfig"]

"""Single-process reliability loop that orders detection, repair, then rebalancing."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from health.failure_detector import FailureDetector, ProbeResult
from repair.worker import RepairRunResult, RepairWorker


@dataclass(frozen=True, slots=True)
class ReliabilityServiceConfig:
    """Scheduling policy for the combined detector/repair/rebalance loop."""

    interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")


class ReliabilityService:
    """Run health detection, durability repair, and optional rebalancing sequentially."""

    def __init__(
        self,
        detector: FailureDetector,
        repair_worker: RepairWorker,
        *,
        rebalancer: Any | None = None,
        config: ReliabilityServiceConfig | None = None,
    ) -> None:
        self.detector = detector
        self.repair_worker = repair_worker
        self.rebalancer = rebalancer
        self.config = config or ReliabilityServiceConfig()

    async def run_once(self) -> tuple[tuple[ProbeResult, ...], RepairRunResult]:
        probes = await self.detector.scan_once()
        repair_result = await self.repair_worker.run_once()
        # Repair always precedes rebalancing so an unhealthy durability state
        # cannot be hidden by moving healthy replicas around.
        if self.rebalancer is not None:
            await self.rebalancer.run_once()
        return probes, repair_result

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Run detector, repair, and optional rebalancing on each interval."""
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

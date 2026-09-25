"""Single-process reliability loop that orders detection before repair."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from health.failure_detector import FailureDetector, ProbeResult
from repair.worker import RepairRunResult, RepairWorker


@dataclass(frozen=True, slots=True)
class ReliabilityServiceConfig:
    """Scheduling policy for the combined detector/repair loop."""

    interval_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")


class ReliabilityService:
    """Run health detection and repair sequentially over one SQLAlchemy session."""

    def __init__(
        self,
        detector: FailureDetector,
        repair_worker: RepairWorker,
        *,
        config: ReliabilityServiceConfig | None = None,
    ) -> None:
        self.detector = detector
        self.repair_worker = repair_worker
        self.config = config or ReliabilityServiceConfig()

    async def run_once(self) -> tuple[tuple[ProbeResult, ...], RepairRunResult]:
        probes = await self.detector.scan_once()
        repair_result = await self.repair_worker.run_once()
        return probes, repair_result

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Run detector then repair on each interval until stopped."""
        while not stop_event.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.config.interval_seconds,
                )
            except asyncio.TimeoutError:
                continue


__all__ = [
    "ReliabilityService",
    "ReliabilityServiceConfig",
]

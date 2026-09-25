"""Cooperative health-check scheduler for the Vault control plane."""

from __future__ import annotations

import asyncio
from typing import Optional

from .failure_detector import FailureDetector, NodeTransition


class HealthScheduler:
    """Run failure-detection scans at a bounded periodic interval."""

    def __init__(self, detector: FailureDetector, *, interval_seconds: float = 5.0) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        self.detector = detector
        self.interval_seconds = interval_seconds

    def run_once(self, *, now=None) -> list[NodeTransition]:
        return self.detector.scan(now=now)

    async def run_forever(self, *, stop_event: Optional[asyncio.Event] = None) -> None:
        while True:
            self.run_once()
            if stop_event is None:
                await asyncio.sleep(self.interval_seconds)
                continue
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                continue
            return

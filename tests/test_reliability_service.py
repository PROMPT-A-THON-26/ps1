from __future__ import annotations

import pytest

from worker.reliability_service import ReliabilityService, ReliabilityServiceConfig


class FakeDetector:
    def __init__(self, events):
        self.events = events

    async def scan_once(self):
        self.events.append("detect")
        return ("probe",)


class FakeRepairWorker:
    def __init__(self, events):
        self.events = events

    async def run_once(self):
        self.events.append("repair")
        return "repair-result"


class FakeRebalancer:
    def __init__(self, events):
        self.events = events

    async def run_once(self):
        self.events.append("rebalance")
        return "rebalance-result"


@pytest.mark.asyncio
async def test_reliability_service_runs_detection_repair_then_rebalance():
    events = []
    service = ReliabilityService(
        FakeDetector(events),
        FakeRepairWorker(events),
        rebalancer=FakeRebalancer(events),
        config=ReliabilityServiceConfig(interval_seconds=1),
    )

    probes, repair = await service.run_once()

    assert probes == ("probe",)
    assert repair == "repair-result"
    assert events == ["detect", "repair", "rebalance"]


@pytest.mark.asyncio
async def test_reliability_service_keeps_rebalance_optional():
    events = []
    service = ReliabilityService(
        FakeDetector(events),
        FakeRepairWorker(events),
        config=ReliabilityServiceConfig(interval_seconds=1),
    )

    await service.run_once()

    assert events == ["detect", "repair"]


def test_reliability_service_rejects_non_positive_interval():
    with pytest.raises(ValueError):
        ReliabilityServiceConfig(interval_seconds=0)

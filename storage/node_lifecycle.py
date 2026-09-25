from __future__ import annotations

from enum import StrEnum
from threading import Lock


class NodeLifecycleState(StrEnum):
    HEALTHY = "healthy"
    DRAINING = "draining"


class NodeLifecycle:
    """Thread-safe local lifecycle state for a storage node."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._state = NodeLifecycleState.HEALTHY

    @property
    def state(self) -> NodeLifecycleState:
        with self._lock:
            return self._state

    @property
    def accepting_writes(self) -> bool:
        return self.state is NodeLifecycleState.HEALTHY

    def drain(self) -> None:
        with self._lock:
            self._state = NodeLifecycleState.DRAINING

    def resume(self) -> None:
        with self._lock:
            self._state = NodeLifecycleState.HEALTHY

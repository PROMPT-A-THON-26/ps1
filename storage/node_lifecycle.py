from __future__ import annotations

from enum import StrEnum
from threading import Lock
from typing import Protocol


class NodeStateStore(Protocol):
    def get_state(self) -> str | None: ...

    def set_state(self, state: str) -> None: ...


class NodeLifecycleState(StrEnum):
    HEALTHY = "healthy"
    DRAINING = "draining"


class NodeLifecycle:
    """Thread-safe local lifecycle state with optional durable persistence."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._state = NodeLifecycleState.HEALTHY
        self._store: NodeStateStore | None = None

    @property
    def state(self) -> NodeLifecycleState:
        with self._lock:
            return self._state

    @property
    def accepting_writes(self) -> bool:
        return self.state is NodeLifecycleState.HEALTHY

    def attach_store(self, store: NodeStateStore, *, restore: bool = True) -> None:
        with self._lock:
            self._store = store
            if restore:
                persisted = store.get_state()
                if persisted is not None:
                    try:
                        self._state = NodeLifecycleState(persisted)
                    except ValueError:
                        # Fail closed if durable state is corrupted. A node must
                        # not silently resume writes after an ambiguous restart.
                        self._state = NodeLifecycleState.DRAINING
                        store.set_state(self._state.value)
                else:
                    store.set_state(self._state.value)

    def detach_store(self) -> None:
        with self._lock:
            self._store = None

    def drain(self) -> None:
        with self._lock:
            self._persist_unlocked(NodeLifecycleState.DRAINING)
            self._state = NodeLifecycleState.DRAINING

    def resume(self) -> None:
        with self._lock:
            self._persist_unlocked(NodeLifecycleState.HEALTHY)
            self._state = NodeLifecycleState.HEALTHY

    def _persist_unlocked(self, state: NodeLifecycleState) -> None:
        if self._store is not None:
            self._store.set_state(state.value)

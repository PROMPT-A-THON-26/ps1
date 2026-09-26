"""Small bounded sliding-window rate limiter for single-process HTTP gateways."""
from __future__ import annotations

from collections import deque
from math import ceil
from threading import Lock
import time


class SlidingWindowRateLimiter:
    """Thread-safe, bounded per-client rate limiter.

    This protects a single gateway process from accidental or abusive request
    bursts. For multi-process/multi-replica deployments, an upstream or shared
    limiter should still enforce the cluster-wide limit.
    """

    def __init__(
        self,
        *,
        limit: int = 240,
        window_seconds: float = 60.0,
        max_clients: int = 4096,
    ) -> None:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be greater than zero")
        if max_clients < 1:
            raise ValueError("max_clients must be at least 1")
        self.limit = limit
        self.window_seconds = window_seconds
        self.max_clients = max_clients
        self._events: dict[str, deque[float]] = {}
        self._lock = Lock()

    def allow(self, client_key: str, *, now: float | None = None) -> tuple[bool, int]:
        if not isinstance(client_key, str) or not client_key:
            raise ValueError("client_key must be a non-empty string")
        if self.limit == 0:
            return True, 0

        timestamp = time.monotonic() if now is None else now
        cutoff = timestamp - self.window_seconds
        with self._lock:
            events = self._events.setdefault(client_key, deque())
            while events and events[0] <= cutoff:
                events.popleft()

            if len(events) >= self.limit:
                retry_after = max(1, ceil(self.window_seconds - (timestamp - events[0])))
                return False, retry_after

            events.append(timestamp)
            if len(self._events) > self.max_clients:
                self._evict_one_empty_client()
            return True, 0

    def _evict_one_empty_client(self) -> None:
        for key, events in self._events.items():
            if not events:
                del self._events[key]
                return
        # No empty client exists; boundedness matters more than retaining the
        # oldest identity, so remove one deterministic entry.
        self._events.pop(next(iter(self._events)))

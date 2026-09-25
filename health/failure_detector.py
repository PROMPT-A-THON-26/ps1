"""Heartbeat timeout detection for storage-node membership health."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from common.constants import DEFAULT_SUSPECT_AFTER_SECONDS, DEFAULT_UNAVAILABLE_AFTER_SECONDS, NodeState
from metadata.models import StorageNode

from .node_registry import NodeRegistry


@dataclass(frozen=True, slots=True)
class NodeTransition:
    node_id: str
    previous: NodeState
    current: NodeState


class FailureDetector:
    """Convert missed heartbeats into SUSPECT/UNAVAILABLE states."""

    def __init__(
        self,
        session,
        *,
        suspect_after_seconds: float = DEFAULT_SUSPECT_AFTER_SECONDS,
        unavailable_after_seconds: float = DEFAULT_UNAVAILABLE_AFTER_SECONDS,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        if suspect_after_seconds <= 0:
            raise ValueError("suspect_after_seconds must be greater than zero")
        if unavailable_after_seconds <= suspect_after_seconds:
            raise ValueError("unavailable_after_seconds must be greater than suspect_after_seconds")
        self.registry = NodeRegistry(session)
        self.suspect_after_seconds = suspect_after_seconds
        self.unavailable_after_seconds = unavailable_after_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _evaluate(self, node: StorageNode, now: datetime) -> Optional[NodeTransition]:
        if node.status in {NodeState.DRAINING, NodeState.REMOVED, NodeState.JOINING}:
            return None
        if node.last_heartbeat_at is None:
            return None

        elapsed = (now - self._utc(node.last_heartbeat_at)).total_seconds()
        if elapsed < 0:
            return None

        if node.status is NodeState.HEALTHY and elapsed >= self.suspect_after_seconds:
            updated = self.registry.mark_suspect(node.node_id)
            return NodeTransition(node.node_id, NodeState.HEALTHY, updated.status)

        if node.status is NodeState.SUSPECT and elapsed >= self.unavailable_after_seconds:
            updated = self.registry.mark_unavailable(node.node_id)
            return NodeTransition(node.node_id, NodeState.SUSPECT, updated.status)

        if node.status is NodeState.RECOVERING and elapsed >= self.suspect_after_seconds:
            updated = self.registry.mark_suspect(node.node_id)
            return NodeTransition(node.node_id, NodeState.RECOVERING, updated.status)

        return None

    def check_node(self, node_id: str, *, now: Optional[datetime] = None) -> Optional[NodeTransition]:
        node = self.registry.get(node_id)
        current = self._utc(now or self.clock())
        return self._evaluate(node, current)

    def scan(self, now: Optional[datetime] = None) -> list[NodeTransition]:
        current = self._utc(now or self.clock())
        transitions: list[NodeTransition] = []
        for node in self.registry.list_nodes():
            transition = self._evaluate(node, current)
            if transition is not None:
                transitions.append(transition)
        return transitions

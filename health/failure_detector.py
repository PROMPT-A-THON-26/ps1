"""Heartbeat-based storage-node failure detection."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState
from metadata.manager import MetadataManager
from metadata.models import StorageNode


class FailureDetector:
    """Conservatively classify nodes from their last successful heartbeat."""

    def __init__(
        self,
        session: Session,
        *,
        suspect_after_seconds: float = 15.0,
        unavailable_after_seconds: float = 30.0,
    ) -> None:
        if suspect_after_seconds <= 0:
            raise ValueError("suspect_after_seconds must be greater than zero")
        if unavailable_after_seconds <= suspect_after_seconds:
            raise ValueError(
                "unavailable_after_seconds must be greater than suspect_after_seconds"
            )
        self.manager = MetadataManager(session)
        self.suspect_after = timedelta(seconds=suspect_after_seconds)
        self.unavailable_after = timedelta(seconds=unavailable_after_seconds)

    def evaluate(self, *, now: datetime | None = None) -> dict[str, NodeState]:
        """Evaluate every eligible node and persist any state transitions."""
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)

        changes: dict[str, NodeState] = {}
        nodes = list(self.manager.session.scalars(select(StorageNode)))
        for node in nodes:
            if node.status in {NodeState.JOINING, NodeState.DRAINING, NodeState.REMOVED}:
                continue
            if node.last_heartbeat_at is None:
                continue

            heartbeat = node.last_heartbeat_at
            if heartbeat.tzinfo is None:
                heartbeat = heartbeat.replace(tzinfo=timezone.utc)
            age = max(timedelta(0), current - heartbeat)

            if age >= self.unavailable_after:
                target = NodeState.UNAVAILABLE
            elif age >= self.suspect_after:
                target = NodeState.SUSPECT
            elif node.status is NodeState.UNAVAILABLE:
                target = NodeState.RECOVERING
            elif node.status is NodeState.RECOVERING:
                target = NodeState.HEALTHY
            else:
                target = NodeState.HEALTHY

            if node.status is not target:
                self.manager.update_node_heartbeat(
                    node.node_id,
                    status=target,
                    heartbeat_at=heartbeat,
                )
                changes[node.node_id] = target

        return changes

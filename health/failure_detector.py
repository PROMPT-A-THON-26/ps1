"""Heartbeat probing and deterministic node failure-state transitions."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Final, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState
from metadata.manager import MetadataManager
from metadata.models import StorageNode
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
)


@dataclass(frozen=True, slots=True)
class FailureDetectorConfig:
    """Timing policy for node liveness transitions."""

    interval_seconds: float = 5.0
    suspect_after_seconds: float = 15.0
    unavailable_after_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be greater than zero")
        if self.suspect_after_seconds <= 0:
            raise ValueError("suspect_after_seconds must be greater than zero")
        if self.unavailable_after_seconds <= self.suspect_after_seconds:
            raise ValueError(
                "unavailable_after_seconds must be greater than suspect_after_seconds"
            )


@dataclass(frozen=True, slots=True)
class ProbeResult:
    node_id: str
    state: NodeState
    reachable: bool
    changed: bool
    replica_count_marked_unavailable: int = 0


class FailureDetector:
    """Continuously reconcile control-plane node state with storage-node health."""

    RECOVERY_TRANSITION: Final[NodeState] = NodeState.RECOVERING

    def __init__(
        self,
        session: Session,
        nodes: Mapping[str, StorageNodeClient],
        *,
        config: FailureDetectorConfig | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.session = session
        self.manager = MetadataManager(session)
        self.nodes = dict(nodes)
        self.config = config or FailureDetectorConfig()
        self._now = now or (lambda: datetime.now(timezone.utc))

    async def probe_node(self, node_id: str) -> ProbeResult:
        node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == node_id)
        )
        if node is None:
            raise ValueError(f"unknown storage node: {node_id}")

        client = self.nodes.get(node_id)
        if client is None:
            raise ValueError(f"no storage-node client configured for {node_id}")

        now = self._normalize_now(self._now())
        try:
            health = await client.health()
            if health.node_id != node_id:
                raise StorageNodeClientError(
                    f"storage node identity mismatch: expected {node_id}, got {health.node_id}"
                )
            stats = await client.stats()

            remote_status = health.status.strip().lower()
            if remote_status == "draining":
                next_state = NodeState.DRAINING
            elif node.status is NodeState.UNAVAILABLE:
                next_state = self.RECOVERY_TRANSITION
            elif node.status is NodeState.RECOVERING:
                next_state = NodeState.HEALTHY
            else:
                next_state = NodeState.HEALTHY

            changed = next_state is not node.status
            self.manager.update_node_heartbeat(
                node_id,
                capacity_bytes=stats.capacity_bytes,
                used_bytes=stats.used_bytes,
                status=next_state,
                heartbeat_at=now,
            )
            return ProbeResult(
                node_id=node_id,
                state=next_state,
                reachable=True,
                changed=changed,
            )
        except StorageNodeClientError:
            if node.status is NodeState.DRAINING:
                return ProbeResult(
                    node_id=node_id,
                    state=NodeState.DRAINING,
                    reachable=False,
                    changed=False,
                )

            next_state = self._state_for_missed_heartbeat(node, now)
            changed = next_state is not node.status
            self.manager.set_node_status(node_id, next_state)

            marked = 0
            if next_state is NodeState.UNAVAILABLE:
                marked = self.manager.mark_node_replicas_unavailable(node_id)

            return ProbeResult(
                node_id=node_id,
                state=next_state,
                reachable=False,
                changed=changed,
                replica_count_marked_unavailable=marked,
            )

    async def scan_once(self) -> tuple[ProbeResult, ...]:
        results = await asyncio.gather(
            *(self.probe_node(node_id) for node_id in self.nodes)
        )
        return tuple(results)

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Run one probe cycle per configured interval until stop_event is set."""
        while not stop_event.is_set():
            await self.scan_once()
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.config.interval_seconds,
                )
            except asyncio.TimeoutError:
                continue

    def _state_for_missed_heartbeat(
        self,
        node: StorageNode,
        now: datetime,
    ) -> NodeState:
        heartbeat = node.last_heartbeat_at
        if heartbeat is None:
            return NodeState.UNAVAILABLE

        heartbeat = self._normalize_now(heartbeat)
        elapsed = max(0.0, (now - heartbeat).total_seconds())
        if elapsed >= self.config.unavailable_after_seconds:
            return NodeState.UNAVAILABLE
        if elapsed >= self.config.suspect_after_seconds:
            return NodeState.SUSPECT
        return node.status

    @staticmethod
    def _normalize_now(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

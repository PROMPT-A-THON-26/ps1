"""Heartbeat timeout detection for storage-node membership health."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from datetime import datetime, timezone
from typing import Callable, Optional
from uuid import UUID

from sqlalchemy import select

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import VaultError
from common.settings import settings
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode, Version
from repair import RepairManager

from .node_registry import NodeRegistry


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class NodeTransition:
    node_id: str
    previous: NodeState
    current: NodeState
    repair_ids: tuple[UUID, ...] = ()


class FailureDetector:
    """Convert missed heartbeats into SUSPECT/UNAVAILABLE states."""

    def __init__(
        self,
        session,
        *,
        suspect_after_seconds: float | None = None,
        unavailable_after_seconds: float | None = None,
        clock: Optional[Callable[[], datetime]] = None,
        repair_manager_factory=RepairManager,
        replication_factor: int | None = None,
    ) -> None:
        suspect_after_seconds = settings.suspect_after_seconds if suspect_after_seconds is None else suspect_after_seconds
        unavailable_after_seconds = settings.unavailable_after_seconds if unavailable_after_seconds is None else unavailable_after_seconds
        replication_factor = settings.replication_factor if replication_factor is None else replication_factor
        if suspect_after_seconds <= 0:
            raise ValueError("suspect_after_seconds must be greater than zero")
        if unavailable_after_seconds <= suspect_after_seconds:
            raise ValueError("unavailable_after_seconds must be greater than suspect_after_seconds")
        self.registry = NodeRegistry(session)
        self.suspect_after_seconds = suspect_after_seconds
        self.unavailable_after_seconds = unavailable_after_seconds
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.session = session
        self.repair_manager_factory = repair_manager_factory
        self.replication_factor = replication_factor

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _schedule_repairs_for_node(self, node_id: str) -> tuple[UUID, ...]:
        repair_ids: list[UUID] = []
        replicas = list(
            self.session.scalars(
                select(Replica)
                .join(Version, Version.version_id == Replica.version_id)
                .where(
                    Replica.node_id == node_id,
                    Replica.status == ReplicaState.HEALTHY,
                    Version.state == VersionState.COMMITTED,
                )
                .order_by(Replica.version_id.asc())
            ).all()
        )
        for replica in replicas:
            MetadataManager(self.session).set_replica_state(
                replica.replica_id, ReplicaState.UNAVAILABLE
            )
            try:
                job = self.repair_manager_factory(self.session).schedule_for_version(
                    replica.version_id,
                    replication_factor=self.replication_factor,
                    reason="node-unavailable",
                )
            except VaultError as exc:
                logger.warning(
                    "Repair scheduling was blocked for version %s on unavailable node %s: %s",
                    replica.version_id,
                    node_id,
                    exc.message,
                )
                continue
            if job is not None:
                repair_ids.append(job.repair_id)
        return tuple(repair_ids)

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
            repair_ids = self._schedule_repairs_for_node(updated.node_id)
            return NodeTransition(
                node.node_id,
                NodeState.SUSPECT,
                updated.status,
                repair_ids=repair_ids,
            )

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

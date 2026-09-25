"""Heartbeat payload validation and node health state updates."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import NodeState, VersionState
from common.errors import InvalidState
from metadata.manager import MetadataManager
from metadata.models import Replica, Version
from recovery import PartitionRecoveryManager


class HeartbeatPayload(BaseModel):
    """Wire-level heartbeat contract sent by a storage node."""

    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(min_length=1)
    capacity_bytes: int = Field(ge=0)
    used_bytes: int = Field(ge=0)
    timestamp: datetime

    @field_validator("node_id")
    @classmethod
    def normalize_node_id(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("node_id must not be empty")
        return value

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("used_bytes")
    @classmethod
    def validate_used_bytes(cls, value: int, info) -> int:
        capacity = info.data.get("capacity_bytes")
        if capacity is not None and value > capacity:
            raise ValueError("used_bytes cannot exceed capacity_bytes")
        return value


class HeartbeatResult(BaseModel):
    """Result returned to a node after heartbeat processing."""

    model_config = ConfigDict(from_attributes=True)

    node_id: str
    accepted: bool
    status: NodeState
    capacity_bytes: int
    used_bytes: int
    free_bytes: int
    last_heartbeat_at: Optional[datetime]


class HeartbeatService:
    """Accept only newer heartbeats and drive controlled recovery."""

    def __init__(
        self,
        session: Session,
        *,
        recovery_manager_factory=PartitionRecoveryManager,
        replication_factor: int = 3,
    ) -> None:
        self.session = session
        self.manager = MetadataManager(session)
        self.recovery_manager_factory = recovery_manager_factory
        self.replication_factor = replication_factor

    def _has_committed_replicas(self, node_id: str) -> bool:
        count = self.session.scalar(
            select(func.count(Replica.replica_id))
            .join(Version, Version.version_id == Replica.version_id)
            .where(
                Replica.node_id == node_id,
                Version.state == VersionState.COMMITTED,
                Replica.status != "FAILED",
            )
        )
        return bool(count)

    def _result(self, node) -> HeartbeatResult:
        return HeartbeatResult(
            node_id=node.node_id,
            accepted=True,
            status=node.status,
            capacity_bytes=node.capacity_bytes,
            used_bytes=node.used_bytes,
            free_bytes=node.free_bytes,
            last_heartbeat_at=node.last_heartbeat_at,
        )

    def ingest(self, payload: HeartbeatPayload) -> HeartbeatResult:
        """Process a heartbeat; recovered nodes with data remain RECOVERING until reconciliation."""
        node, accepted = self.manager.process_node_heartbeat(
            payload.node_id,
            capacity_bytes=payload.capacity_bytes,
            used_bytes=payload.used_bytes,
            heartbeat_at=payload.timestamp,
        )

        if accepted:
            if node.status in {NodeState.JOINING, NodeState.SUSPECT}:
                node = self.manager.transition_node_state(
                    node.node_id,
                    NodeState.HEALTHY,
                )
            elif node.status is NodeState.UNAVAILABLE:
                node = self.manager.transition_node_state(
                    node.node_id,
                    NodeState.RECOVERING,
                )
                if not self._has_committed_replicas(node.node_id):
                    node = self.manager.transition_node_state(
                        node.node_id,
                        NodeState.HEALTHY,
                    )
            elif node.status is NodeState.RECOVERING:
                if not self._has_committed_replicas(node.node_id):
                    node = self.manager.transition_node_state(
                        node.node_id,
                        NodeState.HEALTHY,
                    )
            elif node.status is NodeState.REMOVED:
                raise InvalidState(
                    f"Removed node {node.node_id} must be registered again before heartbeat."
                )

        return HeartbeatResult(
            node_id=node.node_id,
            accepted=accepted,
            status=node.status,
            capacity_bytes=node.capacity_bytes,
            used_bytes=node.used_bytes,
            free_bytes=node.free_bytes,
            last_heartbeat_at=node.last_heartbeat_at,
        )

    async def ingest_and_recover(self, payload: HeartbeatPayload) -> HeartbeatResult:
        """Process a heartbeat and reconcile a recovered node before HEALTHY."""
        result = self.ingest(payload)
        if not result.accepted or result.status is not NodeState.RECOVERING:
            return result

        recovery = self.recovery_manager_factory(
            self.session,
            replication_factor=self.replication_factor,
        )
        recovery_result = await recovery.reconcile_and_mark_healthy(result.node_id)
        node = self.manager._lock_object(
            __import__("uuid").UUID(int=0)
        ) if False else None
        refreshed = self.session.scalar(
            select(type(self.manager._lock_object) if False else Version)
        ) if False else self.manager.get_object("") if False else None
        node = self.session.scalar(
            select(__import__("metadata.models", fromlist=["StorageNode"]).StorageNode).where(
                __import__("metadata.models", fromlist=["StorageNode"]).StorageNode.node_id == result.node_id
            )
        )
        if node is None:
            raise InvalidState(f"Recovered node {result.node_id} disappeared during reconciliation.")
        return HeartbeatResult(
            node_id=node.node_id,
            accepted=True,
            status=node.status,
            capacity_bytes=node.capacity_bytes,
            used_bytes=node.used_bytes,
            free_bytes=node.free_bytes,
            last_heartbeat_at=node.last_heartbeat_at,
        )

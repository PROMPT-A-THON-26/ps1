"""Heartbeat payload validation and node health state updates."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.orm import Session

from common.constants import NodeState
from metadata.manager import MetadataManager


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

    def __init__(self, session: Session) -> None:
        self.session = session
        self.manager = MetadataManager(session)

    def ingest(self, payload: HeartbeatPayload) -> HeartbeatResult:
        node, accepted = self.manager.process_node_heartbeat(
            payload.node_id,
            capacity_bytes=payload.capacity_bytes,
            used_bytes=payload.used_bytes,
            heartbeat_at=payload.timestamp,
        )

        if accepted:
            if node.status in {NodeState.JOINING, NodeState.SUSPECT}:
                node = self.manager.transition_node_state(node.node_id, NodeState.HEALTHY)
            elif node.status is NodeState.UNAVAILABLE:
                node = self.manager.transition_node_state(node.node_id, NodeState.RECOVERING)
            elif node.status is NodeState.RECOVERING:
                node = self.manager.transition_node_state(node.node_id, NodeState.HEALTHY)
            elif node.status is NodeState.REMOVED:
                from common.errors import InvalidState
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

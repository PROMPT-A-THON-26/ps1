"""Transactional node registry operations for the Vault control plane."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState
from common.errors import ObjectNotFound
from metadata.manager import MetadataManager
from metadata.models import StorageNode


class NodeRegistry:
    """Facade for cluster membership, lifecycle transitions, and placement queries."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self.manager = MetadataManager(session)

    def register(self, *, node_id: Optional[str] = None, address: str, capacity_bytes: int = 0) -> StorageNode:
        return self.manager.register_node(
            node_id=node_id,
            address=address,
            capacity_bytes=capacity_bytes,
            status=NodeState.JOINING,
        )

    def get(self, node_id: str) -> StorageNode:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        node = self.session.scalar(select(StorageNode).where(StorageNode.node_id == node_id.strip()))
        if node is None:
            raise ObjectNotFound(node_id.strip())
        return node

    def list_nodes(self, status: Optional[NodeState] = None) -> list[StorageNode]:
        if status is not None and not isinstance(status, NodeState):
            raise ValueError("status must be a NodeState")
        stmt = select(StorageNode).order_by(StorageNode.node_id)
        if status is not None:
            stmt = stmt.where(StorageNode.status == status)
        return list(self.session.scalars(stmt).all())

    def healthy_nodes(
        self,
        *,
        min_free_bytes: int = 0,
        exclude_node_ids: Iterable[str] = (),
    ) -> list[StorageNode]:
        if (
            not isinstance(min_free_bytes, int)
            or isinstance(min_free_bytes, bool)
            or min_free_bytes < 0
        ):
            raise ValueError("min_free_bytes must be a non-negative integer")

        excluded = {
            value.strip()
            for value in exclude_node_ids
            if isinstance(value, str) and value.strip()
        }
        statement = (
            select(StorageNode)
            .where(
                StorageNode.status == NodeState.HEALTHY,
                StorageNode.capacity_bytes - StorageNode.used_bytes >= min_free_bytes,
            )
            .order_by(
                (StorageNode.capacity_bytes - StorageNode.used_bytes).desc(),
                StorageNode.node_id.asc(),
            )
        )
        if excluded:
            statement = statement.where(StorageNode.node_id.not_in(excluded))
        return list(self.session.scalars(statement).all())

    def transition(self, node_id: str, state: NodeState) -> StorageNode:
        return self.manager.transition_node_state(node_id, state)

    def mark_joined(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.HEALTHY)

    def mark_healthy(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.HEALTHY)

    def mark_suspect(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.SUSPECT)

    def mark_unavailable(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.UNAVAILABLE)

    def mark_recovering(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.RECOVERING)

    def drain(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.DRAINING)

    def remove(self, node_id: str) -> StorageNode:
        return self.transition(node_id, NodeState.REMOVED)

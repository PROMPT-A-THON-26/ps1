"""Deterministic, capacity-aware placement for Vault replicas."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import DEFAULT_REPLICATION_FACTOR, ErrorCode, NodeState
from common.settings import settings
from common.errors import VaultError
from metadata.models import Replica, StorageNode


@dataclass(frozen=True, slots=True)
class PlacementPolicy:
    """Replication placement policy supplied by application configuration."""

    replication_factor: int = DEFAULT_REPLICATION_FACTOR

    @classmethod
    def from_settings(cls) -> "PlacementPolicy":
        return cls(replication_factor=settings.replication_factor)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.replication_factor, int)
            or isinstance(self.replication_factor, bool)
            or self.replication_factor < 1
        ):
            raise ValueError("replication_factor must be a positive integer")


class PlacementManager:
    """Select eligible storage nodes without reserving or mutating capacity."""

    def __init__(
        self,
        session: Session,
        *,
        policy: PlacementPolicy | None = None,
    ) -> None:
        self.session = session
        self.policy = policy or PlacementPolicy.from_settings()

    @staticmethod
    def _validate_version_id(version_id: UUID) -> None:
        if not isinstance(version_id, UUID):
            raise ValueError("version_id must be a UUID")

    @staticmethod
    def _validate_size(size_bytes: int) -> None:
        if (
            not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes < 0
        ):
            raise ValueError("size_bytes must be a non-negative integer")

    @staticmethod
    def _validate_factor(replication_factor: int) -> None:
        if (
            not isinstance(replication_factor, int)
            or isinstance(replication_factor, bool)
            or replication_factor < 1
        ):
            raise ValueError("replication_factor must be a positive integer")

    def select_nodes(
        self,
        version_id: UUID,
        *,
        size_bytes: int,
        replication_factor: int | None = None,
    ) -> list[StorageNode]:
        """Return exactly N deterministic eligible nodes or raise INSUFFICIENT_REPLICAS.

        Eligibility follows the Part B contract: HEALTHY nodes only, enough free
        capacity for the object version, and no node that already has a replica
        record for the version. Selection is sorted by free capacity descending
        and node_id ascending as a stable tie-breaker.
        """
        self._validate_version_id(version_id)
        self._validate_size(size_bytes)

        factor = (
            self.policy.replication_factor
            if replication_factor is None
            else replication_factor
        )
        self._validate_factor(factor)

        existing_node_ids = set(
            self.session.scalars(
                select(Replica.node_id).where(Replica.version_id == version_id)
            ).all()
        )

        statement = (
            select(StorageNode)
            .where(
                StorageNode.status == NodeState.HEALTHY,
                StorageNode.free_bytes >= size_bytes,
            )
            .order_by(StorageNode.free_bytes.desc(), StorageNode.node_id.asc())
            .limit(factor)
        )
        if existing_node_ids:
            statement = statement.where(
                StorageNode.node_id.not_in(existing_node_ids)
            )
        candidates = list(self.session.scalars(statement).all())

        if len(candidates) < factor:
            raise VaultError(
                code=ErrorCode.INSUFFICIENT_REPLICAS,
                message=(
                    f"Requested {factor} replicas, but only {len(candidates)} "
                    "eligible storage nodes are available."
                ),
                status_code=503,
            )

        return candidates[:factor]

    def plan_for_version(
        self,
        version_id: UUID,
        *,
        size_bytes: int,
        replication_factor: int | None = None,
    ) -> list[str]:
        """Return selected node IDs for callers that only need placement targets."""
        return [
            node.node_id
            for node in self.select_nodes(
                version_id,
                size_bytes=size_bytes,
                replication_factor=replication_factor,
            )
        ]

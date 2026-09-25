"""Replica reconciliation after network partitions heal."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import ObjectNotFound
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode, Version
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageObjectNotFoundError,
)


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    version_id: UUID
    checked: int
    healthy: int
    stale: int
    corrupted: int
    unavailable: int


class PartitionReconciler:
    """Reconcile committed replica metadata with the canonical version checksum.

    A replica is never allowed to define truth during reconciliation. The
    committed Version row is authoritative. A reachable replica whose bytes do
    not match that version is marked CORRUPTED; it is not promoted and cannot
    overwrite another replica. The repair worker can subsequently replace it
    from a verified healthy source.
    """

    def __init__(
        self,
        session: Session,
        nodes: Mapping[str, StorageNodeClient],
    ) -> None:
        self.session = session
        self.manager = MetadataManager(session)
        self.nodes = dict(nodes)

    async def reconcile_version(self, version_id: UUID) -> ReconciliationResult:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        if version.state is not VersionState.COMMITTED:
            raise ValueError(f"Version {version_id} is not committed.")

        replicas = list(
            self.session.scalars(
                select(Replica)
                .where(Replica.version_id == version_id)
                .order_by(Replica.node_id)
            )
        )

        outcomes = await asyncio.gather(
            *(self._check_replica(version, replica) for replica in replicas)
        )

        counts = {
            ReplicaState.HEALTHY: 0,
            ReplicaState.STALE: 0,
            ReplicaState.CORRUPTED: 0,
            ReplicaState.UNAVAILABLE: 0,
        }
        for state in outcomes:
            counts[state] = counts.get(state, 0) + 1

        return ReconciliationResult(
            version_id=version_id,
            checked=len(outcomes),
            healthy=counts[ReplicaState.HEALTHY],
            stale=counts[ReplicaState.STALE],
            corrupted=counts[ReplicaState.CORRUPTED],
            unavailable=counts[ReplicaState.UNAVAILABLE],
        )

    async def reconcile_all(self) -> tuple[ReconciliationResult, ...]:
        versions = list(
            self.session.scalars(
                select(Version)
                .where(Version.state == VersionState.COMMITTED)
                .order_by(Version.created_at, Version.version_number)
            )
        )
        return tuple(
            await asyncio.gather(
                *(self.reconcile_version(version.version_id) for version in versions)
            )
        )

    async def _check_replica(
        self,
        version: Version,
        replica: Replica,
    ) -> ReplicaState:
        client = self.nodes.get(replica.node_id)
        if client is None:
            self._mark_if_possible(replica, ReplicaState.UNAVAILABLE)
            return ReplicaState.UNAVAILABLE

        try:
            health = await client.health()
            if health.node_id != replica.node_id:
                self._mark_if_possible(replica, ReplicaState.UNAVAILABLE)
                return ReplicaState.UNAVAILABLE

            if health.status.lower() != NodeState.HEALTHY.value.lower():
                self._mark_if_possible(replica, ReplicaState.UNAVAILABLE)
                return ReplicaState.UNAVAILABLE

            verified = await client.verify_object(
                str(version.object_id), str(version.version_id)
            )
        except StorageObjectNotFoundError:
            self._mark_if_possible(replica, ReplicaState.UNAVAILABLE)
            return ReplicaState.UNAVAILABLE
        except StorageNodeClientError:
            # The node may be in a transient partition. Do not call its bytes
            # corrupted merely because the control plane cannot reach it.
            self._mark_if_possible(replica, ReplicaState.UNAVAILABLE)
            return ReplicaState.UNAVAILABLE

        if (
            verified.checksum == version.checksum
            and verified.size_bytes == version.size_bytes
        ):
            if replica.status is ReplicaState.UNAVAILABLE:
                self.manager.mark_replica_reconciled(
                    replica.replica_id,
                    checksum=verified.checksum,
                    size_bytes=verified.size_bytes,
                )
            elif replica.status is not ReplicaState.HEALTHY:
                self._mark_healthy(replica, verified.checksum, verified.size_bytes)
            return ReplicaState.HEALTHY

        # A reachable, self-consistent object that disagrees with the
        # authoritative committed Version is divergent/corrupted, not healthy.
        self._mark_if_possible(replica, ReplicaState.CORRUPTED)
        return ReplicaState.CORRUPTED

    def _mark_healthy(self, replica: Replica, checksum: str, size_bytes: int) -> None:
        if replica.status in {
            ReplicaState.COPYING,
            ReplicaState.REPAIRING,
        }:
            self.manager.mark_replica_healthy(
                replica.replica_id,
                checksum=checksum,
                size_bytes=size_bytes,
            )
            return

        if replica.status is ReplicaState.HEALTHY:
            return

        # Reconciliation must not silently revive terminal FAILED metadata.
        # Repair creates a new replica record on a replacement node.
        raise ValueError(
            f"Replica {replica.replica_id} is {replica.status}; repair is required."
        )

    def _mark_if_possible(self, replica: Replica, target: ReplicaState) -> None:
        if replica.status is target:
            return
        allowed = {
            ReplicaState.HEALTHY: {
                ReplicaState.STALE,
                ReplicaState.CORRUPTED,
                ReplicaState.UNAVAILABLE,
            },
            ReplicaState.STALE: {ReplicaState.REPAIRING},
            ReplicaState.CORRUPTED: {ReplicaState.REPAIRING},
            ReplicaState.UNAVAILABLE: {ReplicaState.REPAIRING},
            ReplicaState.PENDING: {ReplicaState.FAILED},
            ReplicaState.COPYING: {ReplicaState.FAILED},
        }
        if target in allowed.get(replica.status, set()):
            self.manager.set_replica_state(replica.replica_id, target)


__all__ = ["PartitionReconciler", "ReconciliationResult"]

"""Background integrity scanner for committed replicas.""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import VersionState
from metadata.models import Version
from replication.node_client import StorageNodeClient
from replication.reconciler import PartitionReconciler, ReconciliationResult


@dataclass(frozen=True, slots=True)
class IntegrityScanResult:
    versions_scanned: int
    replicas_checked: int
    healthy: int
    stale: int
    corrupted: int
    unavailable: int


class IntegrityScanner:
    """Periodically verify committed replicas against canonical metadata.

    The committed Version checksum/size is authoritative. The scanner delegates
    replica classification to PartitionReconciler, so a divergent replica is
    marked CORRUPTED and an unreachable replica is marked UNAVAILABLE without
    ever promoting replica bytes to source-of-truth metadata.
    """

    def __init__(
        self,
        session: Session,
        nodes: Mapping[str, StorageNodeClient],
        *,
        max_versions_per_scan: int | None = None,
    ) -> None:
        if max_versions_per_scan is not None and max_versions_per_scan < 1:
            raise ValueError("max_versions_per_scan must be at least 1")
        self.session = session
        self.nodes = dict(nodes)
        self.max_versions_per_scan = max_versions_per_scan
        self.reconciler = PartitionReconciler(session, self.nodes)

    async def scan_once(self) -> IntegrityScanResult:
        query = (
            select(Version)
            .where(Version.state == VersionState.COMMITTED)
            .order_by(Version.created_at, Version.version_number)
        )
        if self.max_versions_per_scan is not None:
            query = query.limit(self.max_versions_per_scan)

        versions = list(self.session.scalars(query))
        results: list[ReconciliationResult] = []
        for version in versions:
            results.append(await self.reconciler.reconcile_version(version.version_id))

        return IntegrityScanResult(
            versions_scanned=len(results),
            replicas_checked=sum(item.checked for item in results),
            healthy=sum(item.healthy for item in results),
            stale=sum(item.stale for item in results),
            corrupted=sum(item.corrupted for item in results),
            unavailable=sum(item.unavailable for item in results),
        )


__all__ = ["IntegrityScanResult", "IntegrityScanner"]

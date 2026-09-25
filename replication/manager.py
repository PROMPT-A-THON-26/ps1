"""Replication orchestration for Vault object versions."""

from __future__ import annotations

from collections.abc import AsyncIterable, Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from common.constants import (
    DEFAULT_READ_QUORUM,
    DEFAULT_REPLICATION_FACTOR,
    DEFAULT_WRITE_QUORUM,
    ErrorCode,
    ReplicaState,
    VersionState,
)
from common.errors import InvalidState, ObjectNotFound, VaultError
from metadata.manager import MetadataManager
from metadata.models import Replica, StorageNode, Version
from placement import PlacementManager, PlacementPolicy

from .node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageNodeIntegrityError,
    StorageObjectAlreadyExistsError,
)

Payload = bytes | AsyncIterable[bytes]
PayloadFactory = Callable[[], Payload]
ClientFactory = Callable[[str], StorageNodeClient]


@dataclass(frozen=True, slots=True)
class ReplicationPolicy:
    """Configurable replication and quorum policy."""

    factor: int = DEFAULT_REPLICATION_FACTOR
    write_quorum: int = DEFAULT_WRITE_QUORUM
    read_quorum: int = DEFAULT_READ_QUORUM

    def __post_init__(self) -> None:
        for name, value in (
            ("factor", self.factor),
            ("write_quorum", self.write_quorum),
            ("read_quorum", self.read_quorum),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.write_quorum > self.factor:
            raise ValueError("write_quorum must not exceed factor")
        if self.read_quorum > self.factor:
            raise ValueError("read_quorum must not exceed factor")


@dataclass(frozen=True, slots=True)
class ReplicaWriteResult:
    node_id: str
    status: ReplicaState
    verified: bool
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ReplicationResult:
    version_id: UUID
    selected_node_ids: tuple[str, ...]
    healthy_node_ids: tuple[str, ...]
    failed_node_ids: tuple[str, ...]
    committed: bool

    @property
    def healthy_count(self) -> int:
        return len(self.healthy_node_ids)


class ReplicationManager:
    """Coordinate placement, storage writes, verification, and version commit."""

    def __init__(
        self,
        session: Session,
        *,
        policy: ReplicationPolicy | None = None,
        client_factory: ClientFactory = StorageNodeClient,
    ) -> None:
        self.session = session
        self.policy = policy or ReplicationPolicy()
        self.client_factory = client_factory
        self.metadata = MetadataManager(session)
        self.placement = PlacementManager(
            session,
            policy=PlacementPolicy(replication_factor=self.policy.factor),
        )

    def _get_version(self, version_id: UUID) -> Version:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        if version.state is not VersionState.PREPARING:
            raise InvalidState(
                f"Version {version_id} is {version.state}, not PREPARING."
            )
        return version

    def healthy_replica_count(self, version_id: UUID) -> int:
        return int(
            self.session.scalar(
                select(func.count(Replica.replica_id)).where(
                    Replica.version_id == version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
            )
            or 0
        )

    async def _write_one(
        self,
        *,
        version: Version,
        node: StorageNode,
        payload_factory: PayloadFactory,
        replica: Replica,
    ) -> ReplicaWriteResult:
        self.metadata.set_replica_state(replica.replica_id, ReplicaState.COPYING)
        client = self.client_factory(node.address)
        try:
            try:
                await client.put_object(
                    str(version.object_id),
                    str(version.version_id),
                    payload_factory(),
                )
            except StorageObjectAlreadyExistsError:
                pass

            verified = await client.verify_object(
                str(version.object_id),
                str(version.version_id),
            )
            if not verified.verified or not verified.valid:
                raise StorageNodeIntegrityError(
                    "Storage-node verification did not validate the replica."
                )
            if verified.checksum.lower() != version.checksum.lower():
                raise StorageNodeIntegrityError(
                    "Storage-node verification checksum does not match metadata.",
                    detail={"expected": version.checksum, "actual": verified.checksum},
                )
            if verified.size_bytes != version.size_bytes:
                raise StorageNodeIntegrityError(
                    "Storage-node verification size does not match metadata.",
                    detail={"expected": version.size_bytes, "actual": verified.size_bytes},
                )

            self.metadata.mark_replica_healthy(
                replica.replica_id,
                checksum=verified.checksum,
                size_bytes=verified.size_bytes,
            )
            return ReplicaWriteResult(node.node_id, ReplicaState.HEALTHY, True)
        except (StorageNodeClientError, VaultError) as exc:
            self.metadata.set_replica_state(replica.replica_id, ReplicaState.FAILED)
            return ReplicaWriteResult(
                node.node_id,
                ReplicaState.FAILED,
                False,
                str(exc),
            )
        except Exception:
            self.metadata.set_replica_state(replica.replica_id, ReplicaState.FAILED)
            raise
        finally:
            await client.aclose()

    async def replicate_version(
        self,
        version_id: UUID,
        *,
        payload_factory: PayloadFactory,
        replication_factor: int | None = None,
        write_quorum: int | None = None,
        expected_current_version: int | None = None,
    ) -> ReplicationResult:
        """Replicate a prepared version and commit only after WQ is verified."""
        if not callable(payload_factory):
            raise ValueError("payload_factory must be callable")

        factor = self.policy.factor if replication_factor is None else replication_factor
        quorum = self.policy.write_quorum if write_quorum is None else write_quorum
        if not isinstance(factor, int) or isinstance(factor, bool) or factor < 1:
            raise ValueError("replication_factor must be a positive integer")
        if not isinstance(quorum, int) or isinstance(quorum, bool) or quorum < 1:
            raise ValueError("write_quorum must be a positive integer")
        if quorum > factor:
            raise ValueError("write_quorum must not exceed replication_factor")
        if expected_current_version is not None and (
            not isinstance(expected_current_version, int)
            or isinstance(expected_current_version, bool)
            or expected_current_version < 0
        ):
            raise ValueError(
                "expected_current_version must be a non-negative integer or None"
            )

        version = self._get_version(version_id)
        nodes = self.placement.select_nodes(
            version_id,
            size_bytes=version.size_bytes,
            replication_factor=factor,
        )

        results: list[ReplicaWriteResult] = []
        for node in nodes:
            replica = self.metadata.create_replica(version_id, node.node_id)
            results.append(
                await self._write_one(
                    version=version,
                    node=node,
                    payload_factory=payload_factory,
                    replica=replica,
                )
            )

        healthy = tuple(item.node_id for item in results if item.verified)
        failed = tuple(item.node_id for item in results if not item.verified)
        if len(healthy) < quorum:
            self.metadata.fail_version(version_id)
            raise VaultError(
                code=ErrorCode.INSUFFICIENT_REPLICAS,
                message=(
                    f"Write quorum {quorum} was not reached for version {version_id}; "
                    f"only {len(healthy)} replica(s) were verified."
                ),
                status_code=503,
            )

        self.metadata.commit_version(
            version_id,
            expected_current_version=expected_current_version,
        )
        return ReplicationResult(
            version_id=version_id,
            selected_node_ids=tuple(node.node_id for node in nodes),
            healthy_node_ids=healthy,
            failed_node_ids=failed,
            committed=True,
        )
from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterable, Mapping
from dataclasses import dataclass
from typing import Final
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from common.constants import NodeState, ReplicaState, VersionState
from common.errors import InsufficientReplicas, InvalidState, ObjectNotFound
from metadata.manager import MetadataManager
from metadata.models import Object, Replica, Version
from replication.node_client import (
    StorageNodeClient,
    StorageNodeClientError,
    StorageNodeIntegrityError,
    StorageNodeUnavailableError,
)


@dataclass(frozen=True, slots=True)
class ReplicatedWriteResult:
    object_id: UUID
    version_id: UUID
    version_number: int
    size_bytes: int
    checksum: str
    healthy_nodes: tuple[str, ...]
    failed_nodes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RepairResult:
    version_id: UUID
    source_node_id: str
    target_node_id: str
    healthy_replica_count: int


class DistributedWriteCoordinator:
    """Quorum write/read/repair orchestration across storage nodes."""

    DEFAULT_REPLICATION_FACTOR: Final[int] = 3
    DEFAULT_WRITE_QUORUM: Final[int] = 2
    DEFAULT_READ_QUORUM: Final[int] = 1

    def __init__(
        self,
        session: Session,
        nodes: Mapping[str, StorageNodeClient],
        *,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR,
        write_quorum: int = DEFAULT_WRITE_QUORUM,
        read_quorum: int = DEFAULT_READ_QUORUM,
    ) -> None:
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        if not 1 <= write_quorum <= replication_factor:
            raise ValueError("write_quorum must be between 1 and replication_factor")
        if not 1 <= read_quorum <= replication_factor:
            raise ValueError("read_quorum must be between 1 and replication_factor")

        self.session = session
        self.manager = MetadataManager(session)
        self.nodes = dict(nodes)
        self.replication_factor = replication_factor
        self.write_quorum = write_quorum
        self.read_quorum = read_quorum

    async def write_object(
        self,
        name: str,
        data: bytes,
        *,
        expected_current_version: int | None = None,
    ) -> ReplicatedWriteResult:
        if not isinstance(data, bytes):
            raise TypeError("integration write currently requires bytes")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("object name must not be empty")

        checksum = hashlib.sha256(data).hexdigest()
        size_bytes = len(data)
        obj = self.manager.get_object(name)
        if obj is None:
            obj = self.manager.create_object(name)
        version = self.manager.create_version(
            obj.object_id,
            size_bytes=size_bytes,
            checksum=checksum,
            expected_current_version=expected_current_version,
        )

        candidates = await self._healthy_candidates(size_bytes)
        if len(candidates) < self.write_quorum:
            self.manager.fail_version(version.version_id)
            raise InsufficientReplicas(self.write_quorum, len(candidates))

        targets = tuple(candidates[: self.replication_factor])
        for node_id in targets:
            replica = self.manager.create_replica(version.version_id, node_id)
            self.manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)

        outcomes = await asyncio.gather(
            *(self._write_one(version, node_id, data, checksum, size_bytes) for node_id in targets)
        )
        outcome_map = dict(outcomes)
        healthy = tuple(sorted(node_id for node_id, ok in outcome_map.items() if ok))
        failed = tuple(sorted(node_id for node_id, ok in outcome_map.items() if not ok))

        if len(healthy) < self.write_quorum:
            self.manager.fail_version(version.version_id)
            raise InsufficientReplicas(self.write_quorum, len(healthy))

        committed = self.manager.commit_version(
            version.version_id,
            expected_current_version=expected_current_version,
        )
        return ReplicatedWriteResult(
            object_id=committed.object_id,
            version_id=committed.version_id,
            version_number=committed.version_number,
            size_bytes=size_bytes,
            checksum=checksum,
            healthy_nodes=healthy,
            failed_nodes=failed,
        )

    async def read_object(self, name: str) -> bytes:
        obj = self.manager.get_object(name)
        if obj is None:
            raise ObjectNotFound(name)
        if obj.current_version_id is None:
            raise InvalidState(f"Object {obj.object_id} has no committed version.")

        version = self.session.scalar(
            select(Version).where(Version.version_id == obj.current_version_id)
        )
        if version is None or version.state is not VersionState.COMMITTED:
            raise InvalidState(f"Object {obj.object_id} has no readable committed version.")

        replicas = list(
            self.session.scalars(
                select(Replica)
                .where(
                    Replica.version_id == version.version_id,
                    Replica.status == ReplicaState.HEALTHY,
                )
                .order_by(Replica.node_id)
            )
        )
        if len(replicas) < self.read_quorum:
            raise InsufficientReplicas(self.read_quorum, len(replicas))

        successful = 0
        failed_replica_ids: list[UUID] = []
        for replica in replicas:
            client = self.nodes.get(replica.node_id)
            if client is None:
                continue
            try:
                verified = await client.verify_object(
                    str(obj.object_id), str(version.version_id)
                )
                if (
                    verified.checksum != version.checksum
                    or verified.size_bytes != version.size_bytes
                ):
                    raise StorageNodeIntegrityError("Replica does not match metadata.")

                async with client.stream_object(
                    str(obj.object_id), str(version.version_id)
                ) as response:
                    payload = b"".join([chunk async for chunk in response.aiter_bytes()])

                if (
                    len(payload) != version.size_bytes
                    or hashlib.sha256(payload).hexdigest() != version.checksum
                ):
                    raise StorageNodeIntegrityError("Retrieved bytes failed checksum validation.")

                successful += 1
                if successful >= self.read_quorum:
                    await self._enqueue_repairs(version, failed_replica_ids)
                    return payload
            except StorageNodeIntegrityError:
                self.manager.set_replica_state(replica.replica_id, ReplicaState.CORRUPTED)
                failed_replica_ids.append(replica.replica_id)
            except StorageNodeUnavailableError:
                self.manager.set_replica_state(replica.replica_id, ReplicaState.UNAVAILABLE)
                self.manager.update_node_heartbeat(
                    replica.node_id, status=NodeState.UNAVAILABLE
                )
                failed_replica_ids.append(replica.replica_id)
            except StorageNodeClientError:
                self.manager.set_replica_state(replica.replica_id, ReplicaState.STALE)
                failed_replica_ids.append(replica.replica_id)

        raise InsufficientReplicas(self.read_quorum, successful)

    async def repair_version(
        self,
        version_id: UUID,
        failed_node_id: str,
    ) -> RepairResult:
        version = self.session.scalar(
            select(Version).where(Version.version_id == version_id)
        )
        if version is None:
            raise ObjectNotFound(str(version_id))
        if version.state is not VersionState.COMMITTED:
            raise InvalidState(f"Version {version_id} is not committed.")

        failed_replica = self.session.scalar(
            select(Replica)
            .where(
                Replica.version_id == version_id,
                Replica.node_id == failed_node_id,
            )
            .with_for_update()
        )
        if failed_replica is not None and failed_replica.status is ReplicaState.HEALTHY:
            self.manager.set_replica_state(
                failed_replica.replica_id, ReplicaState.UNAVAILABLE
            )
        self.manager.update_node_heartbeat(
            failed_node_id, status=NodeState.UNAVAILABLE
        )

        replicas = list(
            self.session.scalars(select(Replica).where(Replica.version_id == version_id))
        )
        healthy = [replica for replica in replicas if replica.status is ReplicaState.HEALTHY]
        if not healthy:
            raise InsufficientReplicas(1, 0)

        candidates = await self._healthy_candidates(
            version.size_bytes,
            exclude={replica.node_id for replica in replicas},
        )
        if not candidates:
            raise InsufficientReplicas(self.replication_factor, len(healthy))

        source = healthy[0]
        job = self.manager.create_repair_job(
            version_id,
            source_node_id=source.node_id,
            target_node_id=candidates[0],
            reason=f"replace unavailable replica {failed_node_id}",
        )
        job = self.manager.claim_repair_job(job.repair_id)
        target_node_id = job.target_node_id
        source_client = self.nodes[job.source_node_id]
        target_client = self.nodes[target_node_id]

        existing_target_replica = self.session.scalar(
            select(Replica).where(
                Replica.version_id == version_id,
                Replica.node_id == target_node_id,
            )
        )
        replica = (
            existing_target_replica
            if existing_target_replica is not None
            else self.manager.create_replica(version_id, target_node_id)
        )
        self.manager.set_replica_state(replica.replica_id, ReplicaState.COPYING)

        try:
            async with source_client.stream_object(
                str(version.object_id), str(version_id)
            ) as response:

                async def stream() -> AsyncIterable[bytes]:
                    async for chunk in response.aiter_bytes():
                        yield chunk

                await target_client.put_object(
                    str(version.object_id), str(version_id), stream()
                )

            verified = await target_client.verify_object(
                str(version.object_id), str(version_id)
            )
            if (
                verified.checksum != version.checksum
                or verified.size_bytes != version.size_bytes
            ):
                raise StorageNodeIntegrityError(
                    "Repaired replica does not match metadata checksum/size."
                )

            self.manager.mark_replica_healthy(
                replica.replica_id,
                checksum=verified.checksum,
                size_bytes=verified.size_bytes,
            )
        except StorageNodeUnavailableError as exc:
            self.manager.set_replica_state(replica.replica_id, ReplicaState.FAILED)
            self.manager.update_node_heartbeat(
                target_node_id, status=NodeState.UNAVAILABLE
            )
            self.manager.fail_repair_job(job.repair_id, str(exc))
            raise
        except StorageNodeClientError as exc:
            self.manager.set_replica_state(replica.replica_id, ReplicaState.FAILED)
            self.manager.fail_repair_job(job.repair_id, str(exc))
            raise
        except Exception as exc:
            self.manager.set_replica_state(replica.replica_id, ReplicaState.FAILED)
            self.manager.fail_repair_job(job.repair_id, str(exc))
            raise

        self.manager.complete_repair_job(job.repair_id)
        healthy_count = len(
            list(
                self.session.scalars(
                    select(Replica).where(
                        Replica.version_id == version_id,
                        Replica.status == ReplicaState.HEALTHY,
                    )
                )
            )
        )
        if healthy_count < self.replication_factor:
            raise InsufficientReplicas(self.replication_factor, healthy_count)

        return RepairResult(
            version_id=version_id,
            source_node_id=source.node_id,
            target_node_id=target_node_id,
            healthy_replica_count=healthy_count,
        )

    async def _enqueue_repairs(
        self,
        version: Version,
        failed_replica_ids: list[UUID],
    ) -> None:
        if not failed_replica_ids:
            return
        replicas = list(
            self.session.scalars(
                select(Replica).where(Replica.version_id == version.version_id)
            )
        )
        healthy = [replica for replica in replicas if replica.status is ReplicaState.HEALTHY]
        if not healthy:
            return
        targets = await self._healthy_candidates(
            version.size_bytes,
            exclude={replica.node_id for replica in replicas},
        )
        if not targets:
            return
        source = healthy[0].node_id
        for index, _replica_id in enumerate(failed_replica_ids):
            if index >= len(targets):
                break
            self.manager.create_repair_job(
                version.version_id,
                source_node_id=source,
                target_node_id=targets[index],
                reason="read-path detected an unhealthy replica",
            )

    async def _write_one(
        self,
        version: Version,
        node_id: str,
        data: bytes,
        checksum: str,
        size_bytes: int,
    ) -> tuple[str, bool]:
        client = self.nodes[node_id]
        replica_id = self._replica_id(version.version_id, node_id)
        object_id = str(version.object_id)
        version_id = str(version.version_id)

        try:
            await client.put_object(object_id, version_id, data)
            verified = await client.verify_object(object_id, version_id)
            if verified.checksum != checksum or verified.size_bytes != size_bytes:
                raise StorageNodeIntegrityError("Replica verification mismatch.")
            self.manager.mark_replica_healthy(
                replica_id,
                checksum=verified.checksum,
                size_bytes=verified.size_bytes,
            )
            return node_id, True
        except StorageNodeUnavailableError:
            await self._reconcile_or_fail(
                client,
                replica_id,
                node_id,
                object_id,
                version_id,
                checksum,
                size_bytes,
                mark_unavailable=True,
            )
            return node_id, self._replica_is_healthy(replica_id)
        except StorageNodeClientError:
            await self._reconcile_or_fail(
                client,
                replica_id,
                node_id,
                object_id,
                version_id,
                checksum,
                size_bytes,
                mark_unavailable=False,
            )
            return node_id, self._replica_is_healthy(replica_id)
        except Exception:
            self.manager.set_replica_state(replica_id, ReplicaState.FAILED)
            return node_id, False

    async def _reconcile_or_fail(
        self,
        client: StorageNodeClient,
        replica_id: UUID,
        node_id: str,
        object_id: str,
        version_id: str,
        checksum: str,
        size_bytes: int,
        *,
        mark_unavailable: bool,
    ) -> None:
        try:
            verified = await client.verify_object(object_id, version_id)
            if verified.checksum == checksum and verified.size_bytes == size_bytes:
                self.manager.mark_replica_healthy(
                    replica_id,
                    checksum=verified.checksum,
                    size_bytes=verified.size_bytes,
                )
                return
        except StorageNodeClientError:
            pass

        self.manager.set_replica_state(replica_id, ReplicaState.FAILED)
        if mark_unavailable:
            try:
                self.manager.update_node_heartbeat(
                    node_id, status=NodeState.UNAVAILABLE
                )
            except ObjectNotFound:
                pass

    def _replica_is_healthy(self, replica_id: UUID) -> bool:
        replica = self.session.scalar(
            select(Replica).where(Replica.replica_id == replica_id)
        )
        return replica is not None and replica.status is ReplicaState.HEALTHY

    async def _healthy_candidates(
        self,
        size_bytes: int,
        *,
        exclude: set[str] | None = None,
    ) -> list[str]:
        excluded = exclude or set()

        async def inspect(node_id: str, client: StorageNodeClient):
            if node_id in excluded:
                return None
            try:
                health = await client.health()
                if health.status.lower() != NodeState.HEALTHY.value.lower():
                    return None
                stats = await client.stats()
                if stats.free_bytes < size_bytes:
                    return None

                self.manager.update_node_heartbeat(
                    node_id,
                    capacity_bytes=stats.capacity_bytes,
                    used_bytes=stats.used_bytes,
                    status=NodeState.HEALTHY,
                )
                return node_id, stats.free_bytes
            except StorageNodeUnavailableError:
                try:
                    self.manager.update_node_heartbeat(
                        node_id, status=NodeState.UNAVAILABLE
                    )
                except ObjectNotFound:
                    pass
                return None

        inspected = await asyncio.gather(
            *(inspect(node_id, client) for node_id, client in self.nodes.items())
        )
        valid = [item for item in inspected if item is not None]
        valid.sort(key=lambda item: (-item[1], item[0]))
        return [node_id for node_id, _free in valid]

    def _replica_id(self, version_id: UUID, node_id: str) -> UUID:
        replica_id = self.session.scalar(
            select(Replica.replica_id).where(
                Replica.version_id == version_id,
                Replica.node_id == node_id,
            )
        )
        if replica_id is None:
            raise ObjectNotFound(
                f"Replica for version {version_id} on node {node_id}"
            )
        return replica_id

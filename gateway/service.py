"""Application service layer for the public Vault gateway."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
from collections.abc import AsyncIterable
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from common.constants import (
    DEFAULT_CHUNK_SIZE_MB,
    ErrorCode,
    NodeState,
    ObjectState,
    ReplicaState,
    VersionState,
)
from common.validation import normalize_object_name
from common.errors import (
    ObjectAlreadyExists,
    ObjectNotFound,
    VaultError,
)
from metadata.manager import MetadataManager
from metadata.models import Object, Replica, StorageNode, Version
from replication import ReplicationManager, ReplicationPolicy
from replication.node_client import StorageNodeClient, StorageNodeClientError


@dataclass(frozen=True, slots=True)
class StagedUpload:
    path: Path
    size_bytes: int
    checksum: str


class GatewayService:
    """Keep routes thin while coordinating metadata and control-plane services."""

    def __init__(self, session: Session) -> None:
        self.session = session
        self.metadata = MetadataManager(session)

    def list_objects(self) -> list[Object]:
        return list(
            self.session.scalars(
                select(Object)
                .where(Object.state != ObjectState.DELETED)
                .order_by(Object.name)
            ).all()
        )

    def get_object(self, name: str) -> Object:
        normalized_name = normalize_object_name(name)
        obj = self.session.scalar(select(Object).where(Object.name == normalized_name))
        if obj is None:
            raise ObjectNotFound(normalized_name)
        return obj

    def get_live_object(self, name: str) -> Object:
        obj = self.get_object(name)
        if obj.state is not ObjectState.ACTIVE:
            raise ObjectNotFound(obj.name)
        return obj

    def object_metadata(self, name: str) -> dict:
        obj = self.get_live_object(name)
        current = None
        if obj.current_version_id is not None:
            current = self.session.scalar(
                select(Version).where(Version.version_id == obj.current_version_id)
            )
            if current is None:
                raise ValueError(
                    "object current_version_id references a missing version"
                )
        return {
            "object_id": str(obj.object_id),
            "name": obj.name,
            "state": obj.state.value,
            "current_version": None if current is None else current.version_number,
            "current_version_id": (
                None if current is None else str(current.version_id)
            ),
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
        }

    def versions(self, name: str) -> list[dict]:
        obj = self.get_live_object(name)
        versions = self.session.scalars(
            select(Version)
            .where(Version.object_id == obj.object_id)
            .order_by(Version.version_number)
        ).all()
        if not versions:
            return []

        version_ids = [version.version_id for version in versions]
        replica_counts = dict(
            self.session.execute(
                select(Replica.version_id, func.count(Replica.replica_id))
                .where(
                    Replica.version_id.in_(version_ids),
                    Replica.status == ReplicaState.HEALTHY,
                )
                .group_by(Replica.version_id)
            ).all()
        )

        return [
            {
                "version_id": str(version.version_id),
                "version_number": version.version_number,
                "size_bytes": version.size_bytes,
                "checksum": version.checksum,
                "state": version.state.value,
                "healthy_replicas": int(replica_counts.get(version.version_id, 0)),
                "created_at": version.created_at,
                "committed_at": version.committed_at,
            }
            for version in versions
        ]

    def list_nodes(self) -> list[StorageNode]:
        return list(
            self.session.scalars(
                select(StorageNode).order_by(StorageNode.node_id)
            ).all()
        )

    def get_node(self, node_id: str) -> StorageNode:
        if not isinstance(node_id, str) or not node_id.strip():
            raise ValueError("node_id must be a non-empty string")
        node = self.session.scalar(
            select(StorageNode).where(StorageNode.node_id == node_id.strip())
        )
        if node is None:
            raise ObjectNotFound(node_id.strip())
        return node

    def health(self) -> dict:
        counts = {state.value: 0 for state in NodeState}
        rows = self.session.execute(
            select(StorageNode.status, func.count(StorageNode.node_id))
            .group_by(StorageNode.status)
        ).all()
        for node_state, count in rows:
            counts[node_state.value] = int(count)

        total_nodes = sum(counts.values())
        healthy_nodes = counts[NodeState.HEALTHY.value]
        overall = "ok" if total_nodes > 0 and healthy_nodes == total_nodes else "degraded"
        return {"status": overall, "nodes": total_nodes, "node_states": counts}

    def head(self, name: str) -> tuple[Object, Version | None]:
        obj = self.get_live_object(name)
        if obj.current_version_id is None:
            return obj, None
        version = self.session.scalar(
            select(Version).where(Version.version_id == obj.current_version_id)
        )
        if version is None:
            raise ValueError(
                "object current_version_id references a missing version"
            )
        if version.state is not VersionState.COMMITTED:
            return obj, None
        return obj, version

    def read_targets(self, name: str) -> tuple[Object, Version, list[StorageNode]]:
        obj, version = self.head(name)
        if version is None:
            raise ObjectNotFound(obj.name)
        targets = list(
            self.session.scalars(
                select(StorageNode)
                .join(
                    Replica,
                    Replica.node_id == StorageNode.node_id,
                )
                .where(
                    Replica.version_id == version.version_id,
                    Replica.status == ReplicaState.HEALTHY,
                    StorageNode.status == NodeState.HEALTHY,
                )
                .order_by(StorageNode.node_id)
            ).all()
        )
        if not targets:
            raise VaultError(
                code=ErrorCode.NODE_UNAVAILABLE,
                message=f"No healthy replica is available for object '{obj.name}'.",
                status_code=503,
            )
        return obj, version, targets

    @staticmethod
    async def stage_upload(chunks: AsyncIterable[bytes]) -> StagedUpload:
        fd, temp_name = tempfile.mkstemp(prefix="vault-upload-", suffix=".bin")
        path = Path(temp_name)
        size = 0
        digest = hashlib.sha256()
        completed = False
        try:
            with os.fdopen(fd, "wb") as handle:
                async for chunk in chunks:
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        raise ValueError("request body yielded a non-bytes chunk")
                    data = bytes(chunk)
                    if not data:
                        continue
                    digest.update(data)
                    size += len(data)
                    await asyncio.to_thread(handle.write, data)
                await asyncio.to_thread(handle.flush)
                await asyncio.to_thread(os.fsync, handle.fileno())
            completed = True
            return StagedUpload(path=path, size_bytes=size, checksum=digest.hexdigest())
        finally:
            if not completed:
                path.unlink(missing_ok=True)

    @staticmethod
    async def file_chunks(
        path: Path,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE_MB * 1024 * 1024,
    ):
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        with path.open("rb") as handle:
            while True:
                data = await asyncio.to_thread(handle.read, chunk_size)
                if not data:
                    break
                yield data

    async def put_object(
        self,
        name: str,
        chunks: AsyncIterable[bytes],
        *,
        expected_current_version: int | None = None,
        replication_policy: ReplicationPolicy | None = None,
        client_factory=StorageNodeClient,
    ) -> dict:
        normalized_name = normalize_object_name(name)

        staged = await self.stage_upload(chunks)
        created_object = False
        version = None
        succeeded = False
        try:
            obj = self.session.scalar(
                select(Object).where(Object.name == normalized_name)
            )
            if obj is None:
                try:
                    obj = self.metadata.create_object(normalized_name)
                    created_object = True
                except ObjectAlreadyExists:
                    obj = self.get_object(normalized_name)

            version = self.metadata.create_version(
                obj.object_id,
                size_bytes=staged.size_bytes,
                checksum=staged.checksum,
                expected_current_version=expected_current_version,
            )

            policy = replication_policy or ReplicationPolicy()
            result = await ReplicationManager(
                self.session,
                policy=policy,
                client_factory=client_factory,
            ).replicate_version(
                version.version_id,
                payload_factory=lambda: self.file_chunks(staged.path),
                expected_current_version=expected_current_version,
            )
            committed = version
            if committed is None or committed.state is not VersionState.COMMITTED:
                raise VaultError(
                    code=ErrorCode.INTERNAL_ERROR,
                    message="Replication completed without a committed version.",
                    status_code=500,
                )
            # The request session is intentionally not auto-committing. Persist
            # the successful metadata transaction before returning the response.
            self.session.commit()
            succeeded = True
            return {
                "object_id": str(obj.object_id),
                "name": obj.name,
                "version_id": str(committed.version_id),
                "version_number": committed.version_number,
                "size_bytes": committed.size_bytes,
                "checksum": committed.checksum,
                "replicas": list(result.healthy_node_ids),
                "replication_factor": policy.factor,
                "write_quorum": policy.write_quorum,
            }
        finally:
            if not succeeded:
                if version is not None and version.state is VersionState.PREPARING:
                    try:
                        self.metadata.fail_version(version.version_id)
                    except VaultError:
                        pass
                if created_object and "obj" in locals() and obj.current_version_id is None:
                    try:
                        self.session.delete(obj)
                        self.session.commit()
                    except SQLAlchemyError:
                        self.session.rollback()
            staged.path.unlink(missing_ok=True)

    async def delete_object(
        self,
        name: str,
        *,
        client_factory=StorageNodeClient,
    ) -> dict:
        obj = self.get_object(name)
        if obj.state is ObjectState.DELETED:
            return {"name": obj.name, "state": obj.state.value, "deleted_replicas": 0}

        if obj.state is ObjectState.ACTIVE:
            self.metadata.transition_object_state(obj.object_id, ObjectState.DELETING)

        replica_rows = self.session.execute(
            select(Replica, StorageNode, Version)
            .join(Version, Replica.version_id == Version.version_id)
            .outerjoin(StorageNode, StorageNode.node_id == Replica.node_id)
            .where(Version.object_id == obj.object_id)
        ).all()

        deleted = 0
        failures: list[str] = []
        clients: dict[str, StorageNodeClient] = {
            node.address: client_factory(node.address)
            for _, node, _ in replica_rows
            if node is not None
        }

        async def delete_one(replica, node, version):
            if node is None:
                return replica, None, True
            try:
                await clients[node.address].delete_object(
                    str(version.object_id),
                    str(version.version_id),
                )
                return replica, node.node_id, True
            except StorageNodeClientError:
                return replica, node.node_id, False

        try:
            results = await asyncio.gather(
                *(delete_one(replica, node, version) for replica, node, version in replica_rows)
            )
            for replica, node_id, succeeded in results:
                if succeeded:
                    self.session.delete(replica)
                    deleted += 1
                elif node_id is not None:
                    failures.append(node_id)
        finally:
            await asyncio.gather(
                *(client.aclose() for client in clients.values()),
                return_exceptions=True,
            )

        self.session.commit()
        if failures:
            raise VaultError(
                code=ErrorCode.NODE_UNAVAILABLE,
                message=f"Unable to delete replicas on nodes: {', '.join(failures)}",
                status_code=503,
            )

        self.metadata.transition_object_state(obj.object_id, ObjectState.DELETED)
        # Persist the final lifecycle transition before the request session closes.
        self.session.commit()
        return {"name": obj.name, "state": ObjectState.DELETED.value, "deleted_replicas": deleted}

    async def preflight_read_target(
        self,
        targets: list[StorageNode],
        *,
        object_id: str,
        version_id: str,
        client_factory=StorageNodeClient,
    ) -> tuple[StorageNode, list[StorageNode]]:
        for index, node in enumerate(targets):
            client = client_factory(node.address)
            try:
                await client.head_object(object_id, version_id)
                return node, targets[index:]
            except StorageNodeClientError:
                continue
            finally:
                await client.aclose()
        raise VaultError(
            code=ErrorCode.NODE_UNAVAILABLE,
            message="No healthy replica could be contacted for the requested object.",
            status_code=503,
        )

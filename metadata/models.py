"""Canonical metadata models for the Vault control plane."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Enum, ForeignKey, String, Text, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from common.constants import JobStatus, NodeState, ObjectState, ReplicaState, VersionState
from common.ids import new_uuid

from .database import Base


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Object(Base):
    __tablename__ = "objects"

    object_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=new_uuid)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    current_version_id: Mapped[Optional[UUID]] = mapped_column(Uuid(as_uuid=True), nullable=True)
    state: Mapped[ObjectState] = mapped_column(
        Enum(ObjectState, native_enum=False, length=16),
        default=ObjectState.ACTIVE,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    versions: Mapped[list["Version"]] = relationship(
        back_populates="object",
        cascade="all, delete-orphan",
        order_by="Version.version_number",
    )


class Version(Base):
    __tablename__ = "versions"
    __table_args__ = (
        UniqueConstraint("object_id", "version_number", name="uq_versions_object_version_number"),
        CheckConstraint("size_bytes >= 0", name="ck_versions_size_nonnegative"),
    )

    version_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=new_uuid)
    object_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("objects.object_id", ondelete="CASCADE"),
        nullable=False,
    )
    version_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[VersionState] = mapped_column(
        Enum(VersionState, native_enum=False, length=16),
        default=VersionState.PREPARING,
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    committed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    object: Mapped[Object] = relationship(back_populates="versions")
    replicas: Mapped[list["Replica"]] = relationship(
        back_populates="version",
        cascade="all, delete-orphan",
    )


class StorageNode(Base):
    __tablename__ = "storage_nodes"
    __table_args__ = (
        CheckConstraint("capacity_bytes >= 0", name="ck_storage_nodes_capacity_nonnegative"),
        CheckConstraint("used_bytes >= 0", name="ck_storage_nodes_used_nonnegative"),
    )

    node_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    address: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    status: Mapped[NodeState] = mapped_column(
        Enum(NodeState, native_enum=False, length=16),
        default=NodeState.JOINING,
        nullable=False,
    )
    capacity_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    used_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_heartbeat_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    replicas: Mapped[list["Replica"]] = relationship(back_populates="node")

    @property
    def free_bytes(self) -> int:
        return max(0, self.capacity_bytes - self.used_bytes)


class Replica(Base):
    __tablename__ = "replicas"
    __table_args__ = (
        UniqueConstraint("version_id", "node_id", name="uq_replicas_version_node"),
        CheckConstraint("size_bytes IS NULL OR size_bytes >= 0", name="ck_replicas_size_nonnegative"),
    )

    replica_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=new_uuid)
    version_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("versions.version_id", ondelete="CASCADE"),
        nullable=False,
    )
    node_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("storage_nodes.node_id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[ReplicaState] = mapped_column(
        Enum(ReplicaState, native_enum=False, length=16),
        default=ReplicaState.PENDING,
        nullable=False,
    )
    checksum: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    size_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    last_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

    version: Mapped[Version] = relationship(back_populates="replicas")
    node: Mapped[StorageNode] = relationship(back_populates="replicas")


class RepairJob(Base):
    __tablename__ = "repair_jobs"

    repair_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=new_uuid)
    version_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("versions.version_id"), nullable=False)
    source_node_id: Mapped[str] = mapped_column(String(128), ForeignKey("storage_nodes.node_id"), nullable=False)
    target_node_id: Mapped[str] = mapped_column(String(128), ForeignKey("storage_nodes.node_id"), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, native_enum=False, length=16),
        default=JobStatus.PENDING,
        nullable=False,
    )
    attempts: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)


class RebalanceJob(Base):
    __tablename__ = "rebalance_jobs"

    rebalance_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=new_uuid)
    version_id: Mapped[UUID] = mapped_column(Uuid(as_uuid=True), ForeignKey("versions.version_id"), nullable=False)
    source_node_id: Mapped[str] = mapped_column(String(128), ForeignKey("storage_nodes.node_id"), nullable=False)
    target_node_id: Mapped[str] = mapped_column(String(128), ForeignKey("storage_nodes.node_id"), nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, native_enum=False, length=16),
        default=JobStatus.PENDING,
        nullable=False,
    )
    attempts: Mapped[int] = mapped_column(BigInteger, default=0, nullable=False)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False)

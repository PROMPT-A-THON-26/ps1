"""Initial Vault metadata schema."""

from alembic import op
import sqlalchemy as sa

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "objects",
        sa.Column("object_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("current_version_id", sa.Uuid(), nullable=True),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("object_id"),
        sa.UniqueConstraint("name"),
    )
    op.create_index("ix_objects_current_version_id", "objects", ["current_version_id"])

    op.create_table(
        "storage_nodes",
        sa.Column("node_id", sa.String(length=128), nullable=False),
        sa.Column("address", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("capacity_bytes", sa.BigInteger(), nullable=False),
        sa.Column("used_bytes", sa.BigInteger(), nullable=False),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("capacity_bytes >= 0", name="ck_storage_nodes_capacity_nonnegative"),
        sa.CheckConstraint("used_bytes >= 0", name="ck_storage_nodes_used_nonnegative"),
        sa.PrimaryKeyConstraint("node_id"),
        sa.UniqueConstraint("address"),
    )
    op.create_index("ix_storage_nodes_status", "storage_nodes", ["status"])
    op.create_index("ix_storage_nodes_last_heartbeat", "storage_nodes", ["last_heartbeat_at"])

    op.create_table(
        "versions",
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("object_id", sa.Uuid(), nullable=False),
        sa.Column("version_number", sa.BigInteger(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("version_number > 0", name="ck_versions_number_positive"),
        sa.CheckConstraint("size_bytes >= 0", name="ck_versions_size_nonnegative"),
        sa.PrimaryKeyConstraint("version_id"),
        sa.UniqueConstraint("object_id", "version_number", name="uq_versions_object_version_number"),
        sa.ForeignKeyConstraint(["object_id"], ["objects.object_id"], ondelete="CASCADE"),
    )
    op.create_index("ix_versions_object_id", "versions", ["object_id"])
    op.create_index("ix_versions_state", "versions", ["state"])

    with op.batch_alter_table("objects") as batch:
        batch.create_foreign_key(
            "fk_objects_current_version_id",
            "versions",
            ["current_version_id"],
            ["version_id"],
            ondelete="SET NULL",
        )

    op.create_table(
        "replicas",
        sa.Column("replica_id", sa.Uuid(), nullable=False),
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("node_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("checksum", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("last_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("size_bytes IS NULL OR size_bytes >= 0", name="ck_replicas_size_nonnegative"),
        sa.PrimaryKeyConstraint("replica_id"),
        sa.UniqueConstraint("version_id", "node_id", name="uq_replicas_version_node"),
        sa.ForeignKeyConstraint(["version_id"], ["versions.version_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["node_id"], ["storage_nodes.node_id"], ondelete="CASCADE"),
    )
    op.create_index("ix_replicas_version_id", "replicas", ["version_id"])
    op.create_index("ix_replicas_node_id", "replicas", ["node_id"])
    op.create_index("ix_replicas_status", "replicas", ["status"])

    op.create_table(
        "repair_jobs",
        sa.Column("repair_id", sa.Uuid(), nullable=False),
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("source_node_id", sa.String(length=128), nullable=False),
        sa.Column("target_node_id", sa.String(length=128), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempts", sa.BigInteger(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("repair_id"),
        sa.ForeignKeyConstraint(["version_id"], ["versions.version_id"]),
        sa.ForeignKeyConstraint(["source_node_id"], ["storage_nodes.node_id"]),
        sa.ForeignKeyConstraint(["target_node_id"], ["storage_nodes.node_id"]),
    )
    op.create_index("ix_repair_jobs_version_id", "repair_jobs", ["version_id"])
    op.create_index("ix_repair_jobs_status", "repair_jobs", ["status"])

    op.create_table(
        "integrity_jobs",
        sa.Column("integrity_id", sa.Uuid(), nullable=False),
        sa.Column("node_id", sa.String(length=128), nullable=True),
        sa.Column("version_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempts", sa.BigInteger(), nullable=False),
        sa.Column("checked_count", sa.BigInteger(), nullable=False),
        sa.Column("corrupted_count", sa.BigInteger(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("integrity_id"),
        sa.ForeignKeyConstraint(["node_id"], ["storage_nodes.node_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["version_id"], ["versions.version_id"], ondelete="SET NULL"),
    )
    op.create_index("ix_integrity_jobs_status", "integrity_jobs", ["status"])
    op.create_index("ix_integrity_jobs_node_id", "integrity_jobs", ["node_id"])
    op.create_index("ix_integrity_jobs_version_id", "integrity_jobs", ["version_id"])

    op.create_table(
        "rebalance_jobs",
        sa.Column("rebalance_id", sa.Uuid(), nullable=False),
        sa.Column("version_id", sa.Uuid(), nullable=False),
        sa.Column("source_node_id", sa.String(length=128), nullable=False),
        sa.Column("target_node_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempts", sa.BigInteger(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("rebalance_id"),
        sa.ForeignKeyConstraint(["version_id"], ["versions.version_id"]),
        sa.ForeignKeyConstraint(["source_node_id"], ["storage_nodes.node_id"]),
        sa.ForeignKeyConstraint(["target_node_id"], ["storage_nodes.node_id"]),
    )
    op.create_index("ix_rebalance_jobs_version_id", "rebalance_jobs", ["version_id"])
    op.create_index("ix_rebalance_jobs_status", "rebalance_jobs", ["status"])


def downgrade() -> None:
    op.drop_table("rebalance_jobs")
    op.drop_table("integrity_jobs")
    op.drop_table("repair_jobs")
    op.drop_table("replicas")
    with op.batch_alter_table("objects") as batch:
        batch.drop_constraint("fk_objects_current_version_id", type_="foreignkey")
    op.drop_table("versions")
    op.drop_index("ix_objects_current_version_id", table_name="objects")
    op.drop_table("storage_nodes")
    op.drop_table("objects")

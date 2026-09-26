"""Compound indexes for frequent control-plane filtering."""
from alembic import op


revision = "0002_performance_indexes"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_replicas_version_status",
        "replicas",
        ["version_id", "status"],
    )
    op.create_index(
        "ix_replicas_node_status",
        "replicas",
        ["node_id", "status"],
    )
    op.create_index(
        "ix_repair_jobs_status_version",
        "repair_jobs",
        ["status", "version_id"],
    )
    op.create_index(
        "ix_integrity_jobs_status_node",
        "integrity_jobs",
        ["status", "node_id"],
    )
    op.create_index(
        "ix_rebalance_jobs_status_version",
        "rebalance_jobs",
        ["status", "version_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_rebalance_jobs_status_version", table_name="rebalance_jobs")
    op.drop_index("ix_integrity_jobs_status_node", table_name="integrity_jobs")
    op.drop_index("ix_repair_jobs_status_version", table_name="repair_jobs")
    op.drop_index("ix_replicas_node_status", table_name="replicas")
    op.drop_index("ix_replicas_version_status", table_name="replicas")

"""Enforce storage-node used capacity invariant."""
from alembic import op

revision = "0003_storage_capacity_invariant"
down_revision = "0002_performance_indexes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("storage_nodes") as batch_op:
        batch_op.create_check_constraint(
            "ck_storage_nodes_used_within_capacity",
            "used_bytes <= capacity_bytes",
        )


def downgrade() -> None:
    with op.batch_alter_table("storage_nodes") as batch_op:
        batch_op.drop_constraint(
            "ck_storage_nodes_used_within_capacity",
            type_="check",
        )

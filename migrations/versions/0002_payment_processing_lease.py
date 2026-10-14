"""payment processing lease

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("payments", sa.Column("lease_id", sa.Uuid(), nullable=True))
    op.add_column("payments", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_check_constraint(
        op.f("ck_payments_lease_only_while_pending"),
        "payments",
        "status = 'pending' OR (lease_id IS NULL AND lease_expires_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_payments_lease_only_while_pending"), "payments", type_="check")
    op.drop_column("payments", "lease_expires_at")
    op.drop_column("payments", "lease_id")

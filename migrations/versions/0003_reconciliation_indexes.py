"""indexes for reconciliation of stale pending payments

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_payments_pending_created_at",
        "payments",
        ["created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index("ix_outbox_aggregate_id_created_at", "outbox", ["aggregate_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_outbox_aggregate_id_created_at", table_name="outbox")
    op.drop_index("ix_payments_pending_created_at", table_name="payments")

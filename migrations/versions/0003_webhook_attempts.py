"""Persist the webhook attempt budget across redeliveries.

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "payments", sa.Column("webhook_attempts", sa.Integer(), server_default="0", nullable=False)
    )
    op.create_check_constraint(
        "webhook_attempts_range", "payments", "webhook_attempts BETWEEN 0 AND 3"
    )


def downgrade() -> None:
    op.drop_constraint(op.f("ck_payments_webhook_attempts_range"), "payments", type_="check")
    op.drop_column("payments", "webhook_attempts")

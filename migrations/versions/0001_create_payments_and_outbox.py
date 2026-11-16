"""Create payments and transactional outbox.

Revision ID: 0001
Revises: None
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column("status", sa.String(16), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_amount_positive")),
        sa.CheckConstraint(
            "currency IN ('RUB', 'USD', 'EUR')", name=op.f("ck_payments_currency_allowed")
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name=op.f("ck_payments_status_allowed")
        ),
        sa.CheckConstraint(
            "length(idempotency_key) > 0", name=op.f("ck_payments_idempotency_key_not_empty")
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name=op.f("ck_payments_status_processed_at_consistent"),
        ),
        sa.CheckConstraint(
            "processed_at >= created_at", name=op.f("ck_payments_processed_at_not_before_creation")
        ),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
        sa.ForeignKeyConstraint(
            ["payment_id"],
            ["payments.id"],
            name=op.f("fk_outbox_payment_id_payments"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("attempt_count >= 0", name=op.f("ck_outbox_attempt_count_nonnegative")),
    )
    op.create_index(
        "ix_outbox_unpublished_next_attempt_at",
        "outbox",
        ["next_attempt_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_unpublished_next_attempt_at", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payments.db.base import Base


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency_allowed"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status_allowed"),
        CheckConstraint("length(idempotency_key) > 0", name="idempotency_key_not_empty"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="status_processed_at_consistent",
        ),
        CheckConstraint("processed_at >= created_at", name="processed_at_not_before_creation"),
        CheckConstraint("webhook_attempts BETWEEN 0 AND 3", name="webhook_attempts_range"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(Text)
    # DeclarativeBase reserves the Python attribute name "metadata".
    extra_data: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB)
    status: Mapped[str] = mapped_column(String(16), server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    webhook_attempts: Mapped[int] = mapped_column(Integer, server_default="0")


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("attempt_count >= 0", name="attempt_count_nonnegative"),
        Index(
            "ix_outbox_unpublished_next_attempt_at",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    payment_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.id", ondelete="RESTRICT"))
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    attempt_count: Mapped[int] = mapped_column(Integer, server_default="0")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

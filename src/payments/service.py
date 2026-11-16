from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import OutboxEvent, Payment


class IdempotencyConflict(Exception):
    """An existing key was reused with a different payment request."""


async def create_payment(session: AsyncSession, key: str, values: dict) -> Payment:
    async with session.begin():
        statement = (
            insert(Payment)
            .values(idempotency_key=key, **values)
            .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
            .returning(Payment)
        )
        payment = (await session.execute(statement)).scalar_one_or_none()
        if payment is None:
            # READ COMMITTED gives this SELECT a fresh snapshot after the INSERT
            # has waited for a concurrent request to commit its unique key.
            payment = (
                await session.execute(select(Payment).where(Payment.idempotency_key == key))
            ).scalar_one()
            if any(getattr(payment, name) != value for name, value in values.items()):
                raise IdempotencyConflict
        else:
            session.add(
                OutboxEvent(
                    payment_id=payment.id,
                    event_type="payments.new",
                    payload={"payment_id": str(payment.id)},
                )
            )
    return payment

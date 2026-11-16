import asyncio
import random
from uuid import UUID

import httpx
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment, PaymentStatus


class WebhookAttemptsExhausted(Exception):
    """The durable budget has been consumed, including interrupted attempts."""


async def emulate_gateway() -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED


async def process_payment(
    payment_id: UUID, sessions: async_sessionmaker[AsyncSession], client: httpx.AsyncClient
) -> None:
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        raise ValueError("Unknown payment")
    if payment.webhook_sent_at is not None:
        return
    if payment.status == PaymentStatus.PENDING:
        status = await emulate_gateway()
        async with sessions() as session, session.begin():
            # Only one concurrent handler can persist a result for a pending payment.
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id, Payment.status == "pending")
                .values(status=status, processed_at=func.now())
            )
    async with sessions() as session:
        payment = await session.get(Payment, payment_id)
    if payment.webhook_sent_at is not None:
        return
    # Reserve and commit BEFORE the HTTP request. Concurrent deliveries cannot
    # exceed the budget, and a process crash does not reset it. A crash between
    # this commit and HTTP can consume an attempt without actually sending it.
    async with sessions() as session, session.begin():
        attempt = await session.scalar(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.webhook_sent_at.is_(None),
                Payment.webhook_attempts < 3,
            )
            .values(webhook_attempts=Payment.webhook_attempts + 1)
            .returning(Payment.webhook_attempts)
        )
        if attempt is None:
            current = await session.get(Payment, payment_id)
            if current.webhook_sent_at is not None:
                return
            raise WebhookAttemptsExhausted
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "processed_at": payment.processed_at.isoformat(),
    }
    # The receiver can deduplicate retries even if our process crashes after HTTP 2xx.
    async with asyncio.timeout(client.timeout.read or 5):
        async with client.stream(
            "POST",
            payment.webhook_url,
            json=payload,
            headers={"Idempotency-Key": f"payment-result-{payment.id}"},
        ) as response:
            # Only the status matters. Do not buffer an untrusted response body.
            response.raise_for_status()
    async with sessions() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_sent_at.is_(None))
            .values(webhook_sent_at=func.now())
        )

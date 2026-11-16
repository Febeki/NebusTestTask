from collections.abc import AsyncIterator
from secrets import compare_digest
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Security
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from payments.api.schemas import PaymentAccepted, PaymentCreate, PaymentDetails
from payments.db.models import Payment
from payments.service import IdempotencyConflict, create_payment

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def authenticate(
    request: Request, key: Annotated[str | None, Security(api_key_header)]
) -> None:
    expected = request.app.state.settings.api_key.get_secret_value()
    if key is None or not compare_digest(key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Invalid API key")


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


router = APIRouter(prefix="/api/v1/payments", dependencies=[Depends(authenticate)])
Session = Annotated[AsyncSession, Depends(get_session)]


@router.post("", status_code=202, response_model=PaymentAccepted)
async def post_payment(
    body: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[!-~]+$")
    ],
) -> PaymentAccepted:
    try:
        payment = await create_payment(session, idempotency_key, body.persistence_values())
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail="Idempotency key payload mismatch") from exc
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}", response_model=PaymentDetails)
async def get_payment(payment_id: UUID, session: Session) -> PaymentDetails:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return PaymentDetails(
        idempotency_key=payment.idempotency_key,
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.extra_data,
        webhook_url=payment.webhook_url,
        processed_at=payment.processed_at,
    )

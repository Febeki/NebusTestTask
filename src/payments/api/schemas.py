from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, JsonValue

from payments.db.models import PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)

    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2)]
    currency: Literal["RUB", "USD", "EUR"]
    description: Annotated[str, Field(max_length=10000)]
    metadata: dict[str, JsonValue] | None = None
    webhook_url: HttpUrl

    def persistence_values(self) -> dict:
        values = self.model_dump(exclude={"webhook_url", "metadata"})
        values["webhook_url"] = str(self.webhook_url)
        values["extra_data"] = self.metadata
        return values


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetails(PaymentAccepted):
    idempotency_key: str
    amount: Decimal
    currency: str
    description: str
    metadata: dict[str, JsonValue] | None
    webhook_url: str
    processed_at: datetime | None

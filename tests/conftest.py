import os
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete

from payments.config import WorkerSettings
from payments.db.models import OutboxEvent, Payment
from payments.db.session import create_engine, create_session_factory
from payments.service import create_payment


@pytest_asyncio.fixture
async def outbox_db():
    url = os.environ.get("TEST_OUTBOX_DATABASE_URL")
    if not url:
        pytest.skip("TEST_OUTBOX_DATABASE_URL must point to a dedicated migrated test database")
    settings = WorkerSettings(
        database_url=url,
        rabbitmq_url=os.environ.get("TEST_RABBITMQ_URL", "amqp://localhost/"),
        _env_file=None,
    )
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    try:
        async with sessions() as session, session.begin():
            await session.execute(delete(OutboxEvent))
            await session.execute(delete(Payment))
        async with sessions() as session:
            await create_payment(
                session,
                str(uuid4()),
                {
                    "amount": "10.25",
                    "currency": "RUB",
                    "description": "outbox test",
                    "extra_data": None,
                    "webhook_url": "https://example.com/webhook",
                },
            )
        yield sessions, settings
    finally:
        await engine.dispose()

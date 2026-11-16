import asyncio
import os
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import event, func, select

from payments.api.main import create_app
from payments.config import ApiSettings
from payments.db.models import OutboxEvent, Payment


@pytest_asyncio.fixture
async def api():
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL must point to a migrated test database")
    app = create_app(ApiSettings(database_url=database_url, api_key="test-secret", _env_file=None))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield client, app


def request_data():
    return {
        "amount": "10.25",
        "currency": "RUB",
        "description": "Order",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/webhook",
    }


def headers(key):
    return {"X-API-Key": "test-secret", "Idempotency-Key": key}


async def counts(app, key):
    async with app.state.session_factory() as session:
        payments = await session.scalar(
            select(func.count()).select_from(Payment).where(Payment.idempotency_key == key)
        )
        events = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .join(Payment)
            .where(Payment.idempotency_key == key)
        )
        return payments, events


async def test_create_read_replay_and_conflict(api):
    client, app = api
    key = str(uuid4())
    response = await client.post("/api/v1/payments", json=request_data(), headers=headers(key))
    assert response.status_code == 202, response.text
    payment_id = response.json()["payment_id"]
    replay = await client.post("/api/v1/payments", json=request_data(), headers=headers(key))
    assert replay.json() == response.json()
    detail = await client.get(f"/api/v1/payments/{payment_id}", headers=headers(key))
    assert detail.status_code == 200
    assert detail.json()["metadata"] == {"order_id": 42}
    assert detail.json()["amount"] == "10.25"
    assert detail.json()["idempotency_key"] == key
    changed = request_data() | {"amount": "20"}
    conflict = await client.post("/api/v1/payments", json=changed, headers=headers(key))
    assert conflict.status_code == 409
    assert await counts(app, key) == (1, 1)


async def test_concurrent_same_key(api):
    client, app = api
    key = str(uuid4())
    responses = await asyncio.gather(
        *(
            client.post("/api/v1/payments", json=request_data(), headers=headers(key))
            for _ in range(10)
        )
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert await counts(app, key) == (1, 1)


async def test_concurrent_different_payload(api):
    client, app = api
    key = str(uuid4())
    responses = await asyncio.gather(
        *(
            client.post(
                "/api/v1/payments", json=request_data() | {"amount": amount}, headers=headers(key)
            )
            for amount in ("10", "20")
        )
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    assert await counts(app, key) == (1, 1)


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "0"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"currency": "GBP"},
        {"webhook_url": "file:///tmp/file"},
        {"unexpected": True},
    ],
)
async def test_validation(api, change):
    client, app = api
    key = str(uuid4())
    response = await client.post(
        "/api/v1/payments", json=request_data() | change, headers=headers(key)
    )
    assert response.status_code == 422
    assert await counts(app, key) == (0, 0)


async def test_auth_headers_and_not_found(api):
    client, _ = api
    for request_headers in ({}, {"X-API-Key": "wrong"}):
        response = await client.get(f"/api/v1/payments/{uuid4()}", headers=request_headers)
        assert response.status_code == 401
    response = await client.post(
        "/api/v1/payments", json=request_data(), headers={"X-API-Key": "test-secret"}
    )
    assert response.status_code == 422
    response = await client.get(f"/api/v1/payments/{uuid4()}", headers=headers("unused"))
    assert response.status_code == 404


async def test_outbox_failure_rolls_back_payment(api):
    client, app = api
    key = str(uuid4())

    def fail_insert(*args):
        raise RuntimeError("Simulated outbox write failure")

    event.listen(OutboxEvent, "before_insert", fail_insert)
    try:
        with pytest.raises(RuntimeError, match="Simulated outbox"):
            await client.post("/api/v1/payments", json=request_data(), headers=headers(key))
    finally:
        event.remove(OutboxEvent, "before_insert", fail_insert)
    assert await counts(app, key) == (0, 0)

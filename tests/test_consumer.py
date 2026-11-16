import asyncio
import json
import os
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from faststream.rabbit import RabbitQueue
from sqlalchemy import select, update

import payments.consumer as consumer
import payments.processing as processing
from payments.broker import create_broker
from payments.db.models import Payment


async def payment_row(sessions):
    async with sessions() as session:
        return (await session.scalars(select(Payment))).one()


async def test_delivery_and_duplicate(outbox_db, monkeypatch):
    sessions, _ = outbox_db
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(processing, "emulate_gateway", gateway)
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(200)

    payment = await payment_row(sessions)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await processing.process_payment(payment.id, sessions, client)
        await processing.process_payment(payment.id, sessions, client)
    saved = await payment_row(sessions)
    assert saved.status == "succeeded"
    assert saved.processed_at is not None
    assert saved.webhook_sent_at is not None
    assert gateway.await_count == 1
    assert len(calls) == 1
    assert calls[0].headers["Idempotency-Key"] == f"payment-result-{payment.id}"


async def test_webhook_retry_keeps_gateway_result(outbox_db, monkeypatch):
    sessions, _ = outbox_db
    gateway = AsyncMock(return_value="failed")
    monkeypatch.setattr(processing, "emulate_gateway", gateway)
    calls = []

    def receive(request):
        calls.append(json.loads(request.content))
        return httpx.Response(503 if len(calls) == 1 else 204)

    payment = await payment_row(sessions)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await processing.process_payment(payment.id, sessions, client)
        assert (await payment_row(sessions)).webhook_sent_at is None
        await processing.process_payment(payment.id, sessions, client)
    assert gateway.await_count == 1
    assert calls[0] == calls[1]
    assert calls[1]["status"] == "failed"


@pytest.mark.parametrize(
    "malformed, previous_attempts", [(False, 0), (True, 0), (False, 2), (False, 3)]
)
async def test_real_consumer_retry_and_dlq(outbox_db, monkeypatch, malformed, previous_attempts):
    if not os.environ.get("TEST_RABBITMQ_URL"):
        pytest.skip("TEST_RABBITMQ_URL is required")
    sessions, settings = outbox_db
    settings.retry_base_delay = 0.01
    queue = RabbitQueue(f"test.consumer.{uuid4()}", durable=True)
    dlq = RabbitQueue(f"test.dlq.{uuid4()}", durable=True)
    monkeypatch.setattr(consumer, "payments_queue", queue)
    monkeypatch.setattr(consumer, "dead_letter_queue", dlq)
    gateway = AsyncMock(return_value="succeeded")
    monkeypatch.setattr(processing, "emulate_gateway", gateway)
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(503)

    payment = await payment_row(sessions)
    if previous_attempts:
        # A new consumer receives a payment whose previous process already tried.
        async with sessions() as session, session.begin():
            await session.execute(
                update(Payment)
                .where(Payment.id == payment.id)
                .values(webhook_attempts=previous_attempts)
            )
    broker = create_broker(settings)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client, broker:
        raw_dlq = await broker.declare_queue(dlq)
        raw_queue = await broker.declare_queue(queue)
        consumer.register_handler(broker, sessions, client, settings)
        await broker.start()
        message_id = str(uuid4())
        body = {"bad": True} if malformed else {"payment_id": str(payment.id)}
        try:
            await broker.publish(body, queue=queue, message_id=message_id, persist=True)
            async with asyncio.timeout(10):
                while True:
                    message = await raw_dlq.get(fail=False)
                    if message is not None:
                        break
                    await asyncio.sleep(0.02)
            assert message.message_id == message_id
            assert json.loads(message.body) == body
            assert message.headers["attempts"] == 3
            assert len(requests) == (0 if malformed else 3 - previous_attempts)
            assert gateway.await_count == (0 if malformed else 1)
            await message.ack()
        finally:
            await raw_queue.delete(if_unused=False, if_empty=False)
            await raw_dlq.delete(if_unused=False, if_empty=False)


async def test_interrupted_attempt_is_not_reset(outbox_db, monkeypatch):
    sessions, _ = outbox_db
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(return_value="succeeded"))
    payment = await payment_row(sessions)

    def interrupted(request):
        raise asyncio.CancelledError

    async with httpx.AsyncClient(transport=httpx.MockTransport(interrupted)) as client:
        for expected in range(1, 4):
            with pytest.raises(asyncio.CancelledError):
                await processing.process_payment(payment.id, sessions, client)
            assert (await payment_row(sessions)).webhook_attempts == expected
        with pytest.raises(processing.WebhookAttemptsExhausted):
            await processing.process_payment(payment.id, sessions, client)


async def test_webhook_response_body_is_not_read(outbox_db, monkeypatch):
    sessions, _ = outbox_db
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(return_value="succeeded"))

    class UntrustedBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise AssertionError("Webhook body must not be read")
            yield b""  # pragma: no cover

    def receive(request):
        return httpx.Response(200, stream=UntrustedBody())

    payment = await payment_row(sessions)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await processing.process_payment(payment.id, sessions, client)
    assert (await payment_row(sessions)).webhook_sent_at is not None


async def test_concurrent_deliveries_share_webhook_budget(outbox_db, monkeypatch):
    sessions, _ = outbox_db
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(return_value="succeeded"))
    receive = AsyncMock(return_value=httpx.Response(503))
    payment = await payment_row(sessions)
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        results = await asyncio.gather(
            *(processing.process_payment(payment.id, sessions, client) for _ in range(6)),
            return_exceptions=True,
        )
    assert receive.await_count == 3
    assert (await payment_row(sessions)).webhook_attempts == 3
    assert sum(isinstance(result, httpx.HTTPStatusError) for result in results) == 3
    assert sum(isinstance(result, processing.WebhookAttemptsExhausted) for result in results) == 3


async def test_dlq_failure_does_not_repeat_webhook_or_ack(outbox_db, monkeypatch):
    sessions, settings = outbox_db
    settings.retry_base_delay = 0.01
    monkeypatch.setattr(processing, "emulate_gateway", AsyncMock(return_value="succeeded"))

    class BrokerStub:
        publish = AsyncMock(side_effect=RuntimeError("Broker unavailable"))

        def subscriber(self, *args, **kwargs):
            def register(handler):
                self.handler = handler
                return handler

            return register

    class MessageStub:
        message_id = str(uuid4())
        ack = AsyncMock()
        nack = AsyncMock()

    payment = await payment_row(sessions)
    message = MessageStub()
    message.raw_message = type(
        "Raw", (), {"body": json.dumps({"payment_id": str(payment.id)}).encode()}
    )()
    broker = BrokerStub()
    receive = AsyncMock(return_value=httpx.Response(503))
    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        consumer.register_handler(broker, sessions, client, settings)
        await broker.handler(message)
        await broker.handler(message)  # Redelivery after failed DLQ transfer.
    assert receive.await_count == 3
    message.ack.assert_not_awaited()
    assert message.nack.await_count == 2
    message.nack.assert_awaited_with(requeue=True)

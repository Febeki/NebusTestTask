import asyncio
import json
import os
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session

from payments.broker import create_broker
from payments.db.models import OutboxEvent
from payments.outbox import publish_next


async def read_event(sessions):
    async with sessions() as session:
        return (await session.scalars(select(OutboxEvent))).one()


async def test_failure_schedules_retry(outbox_db):
    sessions, _ = outbox_db
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("offline")
    assert await publish_next(sessions, broker)
    record = await read_event(sessions)
    assert record.published_at is None
    assert record.attempt_count == 1
    assert record.next_attempt_at > datetime.now(UTC)
    assert not await publish_next(sessions, broker)
    assert broker.publish.await_count == 1
    async with sessions() as session, session.begin():
        row = await session.get(OutboxEvent, record.id)
        row.next_attempt_at = row.created_at
    broker.publish.side_effect = None
    assert await publish_next(sessions, broker)
    assert (await read_event(sessions)).published_at is not None


async def test_competing_publishers_skip_locked(outbox_db):
    sessions, _ = outbox_db
    entered, release = asyncio.Event(), asyncio.Event()
    broker = AsyncMock()

    async def publish(*args, **kwargs):
        entered.set()
        await release.wait()

    broker.publish.side_effect = publish
    task = asyncio.create_task(publish_next(sessions, broker))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert not await publish_next(sessions, broker)
    finally:
        release.set()
        await task
    assert broker.publish.await_count == 1


async def test_retry_delay_is_capped_at_five_minutes(outbox_db):
    sessions, _ = outbox_db
    async with sessions() as session, session.begin():
        record = (await session.scalars(select(OutboxEvent))).one()
        record.attempt_count = 100
    broker = AsyncMock()
    broker.publish.side_effect = ConnectionError("offline")
    before = datetime.now(UTC)
    await publish_next(sessions, broker)
    record = await read_event(sessions)
    assert 299 <= (record.next_attempt_at - before).total_seconds() <= 302


async def test_publish_then_commit_failure_reuses_message_id(outbox_db):
    sessions, _ = outbox_db
    broker = AsyncMock()

    def fail_commit(session):
        raise RuntimeError("Simulated commit failure")

    event.listen(Session, "before_commit", fail_commit)
    try:
        with pytest.raises(RuntimeError, match="Simulated commit"):
            await publish_next(sessions, broker)
    finally:
        event.remove(Session, "before_commit", fail_commit)
    assert (await read_event(sessions)).published_at is None
    assert await publish_next(sessions, broker)
    calls = broker.publish.await_args_list
    assert len(calls) == 2
    assert calls[0].kwargs["message_id"] == calls[1].kwargs["message_id"]


async def test_real_rabbitmq_delivery(outbox_db, monkeypatch):
    if not os.environ.get("TEST_RABBITMQ_URL"):
        pytest.skip("TEST_RABBITMQ_URL is required")
    from faststream.rabbit import RabbitQueue

    import payments.outbox as outbox_module

    sessions, settings = outbox_db
    queue = RabbitQueue(f"test.outbox.{uuid4()}", durable=True)
    monkeypatch.setattr(outbox_module, "payments_queue", queue)
    broker = create_broker(settings)
    async with broker:
        raw_queue = await broker.declare_queue(queue)
        try:
            assert await publish_next(sessions, broker)
            message = await raw_queue.get(timeout=5)
            record = await read_event(sessions)
            assert json.loads(message.body) == record.payload
            assert message.message_id == str(record.id)
            assert message.delivery_mode == 2
            assert record.published_at is not None
            await message.ack()
        finally:
            await raw_queue.delete(if_unused=False, if_empty=False)


async def test_unroutable_message_stays_unpublished(outbox_db, monkeypatch):
    if not os.environ.get("TEST_RABBITMQ_URL"):
        pytest.skip("TEST_RABBITMQ_URL is required")
    from faststream.rabbit import RabbitQueue

    import payments.outbox as outbox_module

    sessions, settings = outbox_db
    # Deliberately do not declare this queue, so mandatory publication is returned.
    monkeypatch.setattr(outbox_module, "payments_queue", RabbitQueue(f"missing.{uuid4()}"))
    async with create_broker(settings) as broker:
        assert await publish_next(sessions, broker)
    record = await read_event(sessions)
    assert record.published_at is None
    assert record.attempt_count == 1

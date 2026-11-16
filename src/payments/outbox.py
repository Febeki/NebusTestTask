import asyncio
import logging
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.broker import create_broker, payments_queue
from payments.config import WorkerSettings
from payments.db.models import OutboxEvent
from payments.db.session import create_engine, create_session_factory

logger = logging.getLogger(__name__)


async def publish_next(sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker) -> bool:
    """Publish one due event. A crash after publish can cause redelivery."""
    async with sessions() as session, session.begin():
        event = await session.scalar(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.next_attempt_at <= func.now())
            .order_by(OutboxEvent.next_attempt_at, OutboxEvent.created_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        event.attempt_count += 1
        try:
            async with asyncio.timeout(10):
                await broker.publish(
                    event.payload,
                    queue=payments_queue,
                    persist=True,
                    mandatory=True,
                    timeout=5,
                    message_id=str(event.id),
                    message_type=event.event_type,
                )
        except Exception as exc:
            delay = min(2 ** min(event.attempt_count, 9), 300)
            event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
            # Exception messages may include credentials or message contents.
            logger.warning("Outbox event %s publication failed (%s)", event.id, type(exc).__name__)
        else:
            event.published_at = datetime.now(UTC)
    return True


async def publish_loop(
    sessions: async_sessionmaker[AsyncSession], broker: RabbitBroker, interval: float
) -> None:
    while True:
        try:
            handled = await publish_next(sessions, broker)
        except Exception as exc:
            logger.error("Outbox iteration failed (%s)", type(exc).__name__)
            handled = False
        if not handled:
            await asyncio.sleep(interval)


async def run() -> None:
    settings = WorkerSettings()
    engine = create_engine(settings)
    broker = create_broker(settings)
    try:
        async with broker:
            await broker.declare_queue(payments_queue)
            sessions = create_session_factory(engine)
            await publish_loop(sessions, broker, settings.outbox_poll_interval)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

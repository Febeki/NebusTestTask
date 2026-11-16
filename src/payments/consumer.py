import asyncio
import logging
import signal
from contextlib import suppress
from uuid import UUID

import httpx
from faststream import AckPolicy
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.broker import create_broker, dead_letter_queue, payments_queue
from payments.config import WorkerSettings
from payments.db.session import create_engine, create_session_factory
from payments.outbox import publish_loop
from payments.processing import WebhookAttemptsExhausted, process_payment

logger = logging.getLogger(__name__)


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID


def register_handler(
    broker: RabbitBroker,
    sessions: async_sessionmaker[AsyncSession],
    client: httpx.AsyncClient,
    settings: WorkerSettings,
) -> None:
    @broker.subscriber(payments_queue, ack_policy=AckPolicy.MANUAL)
    async def handle(message: RabbitMessage) -> None:
        for attempt in range(1, 4):
            try:
                event = PaymentEvent.model_validate_json(message.raw_message.body)
                await process_payment(event.payment_id, sessions, client)
            except Exception as exc:
                logger.warning(
                    "Message %s attempt %s failed (%s)",
                    message.message_id,
                    attempt,
                    type(exc).__name__,
                )
                if attempt < 3 and not isinstance(exc, WebhookAttemptsExhausted):
                    await asyncio.sleep(settings.retry_base_delay * 2 ** (attempt - 1))
                    continue
                try:
                    async with asyncio.timeout(10):
                        await broker.publish(
                            message.raw_message.body,
                            queue=dead_letter_queue,
                            persist=True,
                            mandatory=True,
                            timeout=5,
                            message_id=message.message_id,
                            headers={"attempts": 3, "error_type": type(exc).__name__},
                        )
                except Exception as dlq_exc:
                    # Keep the original message if the DLQ transfer was not confirmed.
                    logger.error(
                        "DLQ transfer failed for message %s (%s)",
                        message.message_id,
                        type(dlq_exc).__name__,
                    )
                    # Avoid an immediate requeue loop during a broker/DLQ failure.
                    await asyncio.sleep(settings.retry_base_delay)
                    await message.nack(requeue=True)
                    return
            await message.ack()
            return


async def run() -> None:
    settings = WorkerSettings()
    engine = create_engine(settings)
    broker = create_broker(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        async with (
            httpx.AsyncClient(
                timeout=settings.webhook_timeout, follow_redirects=False, trust_env=False
            ) as client,
            broker,
        ):
            await broker.declare_queue(dead_letter_queue)
            await broker.declare_queue(payments_queue)
            sessions = create_session_factory(engine)
            register_handler(broker, sessions, client, settings)
            await broker.start()
            publisher = asyncio.create_task(
                publish_loop(sessions, broker, settings.outbox_poll_interval)
            )
            try:
                await stop.wait()
            finally:
                publisher.cancel()
                with suppress(asyncio.CancelledError):
                    await publisher
    finally:
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())

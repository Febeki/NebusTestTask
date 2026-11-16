from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

from payments.config import WorkerSettings

payments_queue = RabbitQueue("payments.new", durable=True)
dead_letter_queue = RabbitQueue("payments.dlq", durable=True)


def create_broker(settings: WorkerSettings) -> RabbitBroker:
    return RabbitBroker(
        settings.rabbitmq_url.get_secret_value(),
        default_channel=Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1),
        timeout=10,
        graceful_timeout=60,
    )

import logging

from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings


def create_broker(settings: Settings, *, graceful_timeout: float | None = None) -> RabbitBroker:
    """graceful_timeout — сколько при остановке ждать обработчики в работе (нужно только consumer'у).

    publisher_confirms — publish() ждёт подтверждения от брокера;
    on_return_raises + mandatory — неотмаршрутизированное сообщение превращается в ошибку,
    а не теряется молча. Без этого outbox мог бы пометить событие опубликованным зря.
    """
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        graceful_timeout=graceful_timeout,
        logger=logging.getLogger("faststream"),
        log_level=logging.INFO,
    )

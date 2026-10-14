import logging

from faststream.rabbit import Channel, RabbitBroker

from app.config import Settings

PUBLISH_TIMEOUT = 10.0  # ожидание publisher confirm, с


def create_broker(settings: Settings, *, graceful_timeout: float | None = None) -> RabbitBroker:
    """publisher_confirms: publish() ждёт подтверждения брокера; on_return_raises + mandatory:
    неотмаршрутизированное сообщение — ошибка, а не тихая потеря.
    graceful_timeout — сколько при остановке ждать обработчики в работе (нужно consumer'у)."""
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        graceful_timeout=graceful_timeout,
        logger=logging.getLogger("faststream"),
        log_level=logging.INFO,
    )

"""Consumer как ASGI-приложение: `uvicorn app.consumer.main:app`.

Помимо обработки очереди отдаёт GET /health для healthcheck: 204, если соединение с RabbitMQ
живо и подписчик запущен, иначе 503.
"""

from typing import Any

from faststream.asgi import AsgiFastStream, AsgiResponse, get

from app.config import get_settings
from app.consumer.app import create_consumer_app
from app.logging_config import configure_logging


def build_asgi_app() -> AsgiFastStream:
    configure_logging(get_settings().log_level)
    consumer = create_consumer_app()
    broker = consumer.broker
    if broker is None:  # create_consumer_app всегда передаёт брокер
        raise RuntimeError("Consumer app has no broker")

    @get(include_in_schema=False)
    async def health(scope: Any) -> AsgiResponse:
        healthy = await broker.ping(timeout=5.0) and all(sub.running for sub in broker.subscribers)
        return AsgiResponse(status_code=204 if healthy else 503)

    return consumer.as_asgi(asgi_routes=[("/health", health)])


app = build_asgi_app()

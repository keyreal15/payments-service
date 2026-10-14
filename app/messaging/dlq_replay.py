"""Возвращает сообщения из DLQ в основную очередь со сброшенным счётчиком попыток.

Используется после устранения причины сбоя (например, клиент починил webhook):

    docker compose exec -T consumer python -m app.messaging.dlq_replay --limit 100

Берётся не больше сообщений, чем было в DLQ на момент запуска: сообщение, которое
сразу же снова упадёт в DLQ, не будет гоняться по кругу. Обработчик идемпотентен —
для финализированного платежа шлюз повторно не вызывается, повторяется только webhook.
Заголовки x-last-error / x-dead-letter-reason сохраняются как x-replayed-* для разбора.
"""

import argparse
import asyncio
import logging
from typing import Any

from app.config import Settings, get_settings
from app.logging_config import configure_logging
from app.messaging.broker import create_broker
from app.messaging.retry import (
    ATTEMPT_HEADER,
    DEAD_LETTER_REASON_HEADER,
    LAST_ERROR_HEADER,
    retry_policy_from_settings,
    strip_broker_headers,
)
from app.messaging.topology import build_topology, declare_topology

logger = logging.getLogger(__name__)


def _replay_headers(headers: dict[str, Any]) -> dict[str, Any]:
    result = {
        key: value
        for key, value in strip_broker_headers(headers).items()
        if key not in (ATTEMPT_HEADER, LAST_ERROR_HEADER, DEAD_LETTER_REASON_HEADER)
    }
    # Причина: наша (явная публикация в DLX) или нативная RabbitMQ (reject, x-delivery-limit).
    reason = headers.get(DEAD_LETTER_REASON_HEADER) or headers.get("x-first-death-reason")
    if reason is not None:
        result["x-replayed-dead-letter-reason"] = reason
    if LAST_ERROR_HEADER in headers:
        result["x-replayed-last-error"] = headers[LAST_ERROR_HEADER]
    return result


async def replay(settings: Settings, limit: int) -> int:
    topology = build_topology(settings, retry_policy_from_settings(settings))
    broker = create_broker(settings)
    await broker.connect()
    replayed = 0
    try:
        await declare_topology(broker, topology)
        dlq = await broker.declare_queue(topology.dead_letter_queue)
        available = dlq.declaration_result.message_count or 0
        while replayed < min(limit, available):
            message = await dlq.get(no_ack=False, fail=False)
            if message is None:
                break
            try:
                await broker.publish(
                    message.body,
                    exchange=topology.exchange,
                    routing_key=topology.routing_key,
                    headers=_replay_headers(dict(message.headers or {})),
                    message_id=message.message_id,
                    correlation_id=message.correlation_id,
                    message_type=message.type,
                    content_type=message.content_type or "application/json",
                    persist=True,
                    mandatory=True,
                    timeout=settings.rabbitmq_publish_timeout,
                )
            except Exception:
                await message.nack(requeue=True)
                raise
            await message.ack()
            replayed += 1
    finally:
        await broker.stop()
    logger.info("Replayed %d message(s) from %s", replayed, topology.dead_letter_queue.name)
    return replayed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=100, help="максимум сообщений за запуск (по умолчанию 100)")
    args = parser.parse_args()
    settings = get_settings()
    configure_logging(settings.log_level)
    asyncio.run(replay(settings, args.limit))


if __name__ == "__main__":
    main()

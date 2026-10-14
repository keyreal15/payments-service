import asyncio
import logging
from typing import Any

from aiormq.exceptions import PublishError
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitBroker, RabbitExchange
from faststream.rabbit.annotations import RabbitMessage

from app.messaging.retry import (
    ATTEMPT_HEADER,
    DEAD_LETTER_REASON_HEADER,
    LAST_ERROR_HEADER,
    RetryPolicy,
    get_attempt,
    strip_broker_headers,
)
from app.messaging.topology import Topology
from app.schemas import PaymentCreatedEvent
from app.services.errors import PermanentProcessingError, TransientProcessingError
from app.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)

MAX_HEADER_ERROR_LENGTH = 500


class PaymentCreatedHandler:
    """Единственный обработчик очереди payments.new: процессинг -> статус в БД -> webhook.

    Исходы обработки сообщения:
      * успех                                    -> ack;
      * ошибка webhook / шлюза, попытки остались  -> копия в retry-очередь с задержкой (x-attempt + 1), ack;
      * попытки исчерпаны                        -> копия в DLQ с x-last-error, ack;
      * неисправимая ошибка (нет платежа, webhook_url на приватный адрес) -> сразу в DLQ;
      * платёж занят другим обработчиком, БД недоступна -> отсрочка без расхода попытки;
      * не удалось переопубликовать: unroutable  -> reject -> DLX (топология сломана, крутить бессмысленно),
                                       иначе     -> пауза и nack(requeue=True); попытка не засчитывается,
                                                    но расходуется x-delivery-limit очереди (защита от вечного цикла).
    Невалидное тело сообщения отклоняет FastStream (AckPolicy.REJECT_ON_ERROR) -> DLX -> DLQ.
    """

    def __init__(
        self,
        processor: PaymentProcessor,
        broker: RabbitBroker,
        topology: Topology,
        retry_policy: RetryPolicy,
        publish_timeout: float,
    ) -> None:
        self._processor = processor
        self._broker = broker
        self._topology = topology
        self._retry_policy = retry_policy
        self._publish_timeout = publish_timeout

    async def handle(self, event: PaymentCreatedEvent, message: RabbitMessage) -> None:
        attempt = get_attempt(message.headers)
        payment_id = event.payment_id
        max_attempts = self._retry_policy.max_attempts
        logger.info("Processing payment %s (attempt %d/%d)", payment_id, attempt, max_attempts)

        try:
            await self._processor.process(payment_id)
        except PermanentProcessingError as exc:
            logger.error("Payment %s: %s, moving message to DLQ", payment_id, exc)
            await self._dead_letter(message, attempt, exc, reason="permanent_error")
        except TransientProcessingError as exc:
            logger.warning("Payment %s: %s, deferring without consuming an attempt", payment_id, exc)
            await self._retry(message, attempt, exc, consume_attempt=False)
        except Exception as exc:
            # Ошибки webhook, технические сбои шлюза и непредвиденные ошибки тратят попытку.
            if self._retry_policy.should_retry(attempt):
                logger.warning("Payment %s: attempt %d/%d failed: %r", payment_id, attempt, max_attempts, exc)
                await self._retry(message, attempt, exc, consume_attempt=True)
            else:
                logger.error(
                    "Payment %s: attempt %d/%d failed, retries exhausted, moving message to DLQ",
                    payment_id,
                    attempt,
                    max_attempts,
                    exc_info=exc,
                )
                await self._dead_letter(message, attempt, exc, reason="max_attempts_exceeded")

    async def _retry(self, message: RabbitMessage, attempt: int, error: Exception, *, consume_attempt: bool) -> None:
        step = attempt if consume_attempt else 1
        retry_queue = self._topology.retry_queue_for(step)
        await self._republish(
            message,
            exchange=self._topology.retry_exchange,
            routing_key=retry_queue.routing(),
            headers={
                ATTEMPT_HEADER: attempt + 1 if consume_attempt else attempt,
                LAST_ERROR_HEADER: repr(error)[:MAX_HEADER_ERROR_LENGTH],
            },
        )
        logger.info("Retry scheduled in %.1fs", self._retry_policy.delay_for(step))

    async def _dead_letter(self, message: RabbitMessage, attempt: int, error: Exception, *, reason: str) -> None:
        # Публикуем в DLX сами (а не reject), чтобы в DLQ была именно финальная ошибка.
        await self._republish(
            message,
            exchange=self._topology.dead_letter_exchange,
            routing_key=self._topology.dead_letter_routing_key,
            headers={
                ATTEMPT_HEADER: attempt,
                LAST_ERROR_HEADER: repr(error)[:MAX_HEADER_ERROR_LENGTH],
                DEAD_LETTER_REASON_HEADER: reason,
            },
        )

    async def _republish(
        self, message: RabbitMessage, *, exchange: RabbitExchange, routing_key: str, headers: dict[str, Any]
    ) -> None:
        """Публикация копии с publisher confirm до ack оригинала.

        Падение между confirm и ack даст дубль — обработчик идемпотентен.
        """
        try:
            await self._broker.publish(
                message.body,
                exchange=exchange,
                routing_key=routing_key,
                headers=strip_broker_headers(message.headers) | headers,
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                message_type=message.raw_message.type,
                content_type=message.content_type or "application/json",
                persist=True,
                mandatory=True,
                timeout=self._publish_timeout,
            )
        except PublishError as exc:  # Basic.Return: некуда маршрутизировать — топология сломана
            logger.exception("Message is unroutable to %s/%s, rejecting to DLQ", exchange.name, routing_key)
            raise RejectMessage() from exc
        except Exception as exc:
            logger.exception("Failed to republish message, returning it to the queue")
            # Пауза, чтобы при стабильной ошибке публикации не получить горячий цикл передоставок.
            await asyncio.sleep(self._retry_policy.base_delay)
            raise NackMessage(requeue=True) from exc

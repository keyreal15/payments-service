import asyncio
import logging
import uuid
from typing import Any

from aiormq.exceptions import PublishError
from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitBroker, RabbitExchange
from faststream.rabbit.annotations import RabbitMessage

from app.messaging.retry import (
    ATTEMPT_HEADER,
    DEAD_LETTER_REASON_HEADER,
    LAST_ERROR_HEADER,
    LEASE_HEADER,
    RetryPolicy,
    get_attempt,
    get_lease_id,
    strip_broker_headers,
)
from app.messaging.topology import Topology
from app.schemas import PaymentEvent
from app.services.errors import PermanentProcessingError, TransientProcessingError
from app.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)

MAX_HEADER_ERROR_LENGTH = 500


class PaymentEventHandler:
    """Обработчик очереди payments.new: решает судьбу сообщения по исходу обработки.

    успех -> ack; ошибка с оставшимися попытками -> копия в retry-очередь; попытки исчерпаны или ошибка
    неисправима -> копия в DLQ с текстом ошибки; временная помеха (платёж занят, БД недоступна) ->
    отсрочка без расхода попытки. Копия публикуется с confirm до ack оригинала (возможен дубль, не потеря).
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

    async def handle(self, event: PaymentEvent, message: RabbitMessage) -> None:
        attempt = get_attempt(message.headers)
        lease_id = get_lease_id(message.headers)
        payment_id = event.payment_id
        max_attempts = self._retry_policy.max_attempts
        logger.info("Processing payment %s (attempt %d/%d)", payment_id, attempt, max_attempts)

        try:
            await self._processor.process(payment_id, lease_id)
        except PermanentProcessingError as exc:
            logger.error("Payment %s: %s, moving message to DLQ", payment_id, exc)
            await self._dead_letter(message, attempt, exc, reason="permanent_error")
        except TransientProcessingError as exc:
            logger.warning("Payment %s: %s, deferring without consuming an attempt", payment_id, exc)
            await self._retry(message, attempt, lease_id, exc, consume_attempt=False)
        except Exception as exc:
            if self._retry_policy.should_retry(attempt):
                logger.warning("Payment %s: attempt %d/%d failed: %r", payment_id, attempt, max_attempts, exc)
                await self._retry(message, attempt, lease_id, exc, consume_attempt=True)
            else:
                logger.error(
                    "Payment %s: attempt %d/%d failed, retries exhausted, moving message to DLQ",
                    payment_id,
                    attempt,
                    max_attempts,
                    exc_info=exc,
                )
                await self._dead_letter(message, attempt, exc, reason="max_attempts_exceeded")

    async def _retry(
        self, message: RabbitMessage, attempt: int, lease_id: uuid.UUID, error: Exception, *, consume_attempt: bool
    ) -> None:
        step = attempt if consume_attempt else 1
        await self._republish(
            message,
            exchange=self._topology.retry_exchange,
            routing_key=self._topology.retry_queue_for(step).routing(),
            headers={
                ATTEMPT_HEADER: attempt + 1 if consume_attempt else attempt,
                LAST_ERROR_HEADER: repr(error)[:MAX_HEADER_ERROR_LENGTH],
                LEASE_HEADER: str(lease_id),
            },
        )
        logger.info("Retry scheduled in %.1fs", self._retry_policy.delay_for(step))

    async def _dead_letter(self, message: RabbitMessage, attempt: int, error: Exception, *, reason: str) -> None:
        # Публикуем в DLX сами, а не через reject: так в DLQ попадает текст финальной ошибки.
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
        except PublishError as exc:  # unroutable: топология сломана, повторять бессмысленно
            logger.exception("Message is unroutable to %s/%s, rejecting to DLQ", exchange.name, routing_key)
            raise RejectMessage() from exc
        except Exception as exc:
            logger.exception("Failed to republish message, returning it to the queue")
            await asyncio.sleep(self._retry_policy.base_delay)  # без горячего цикла передоставок
            raise NackMessage(requeue=True) from exc

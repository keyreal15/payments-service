"""Топология RabbitMQ (схема — в README).

payments -> payments.new -> consumer; повторы через payments.retry -> payments.new.retry.<N>ms (TTL на уровне
очереди, отдельная очередь на ступень — без head-of-line blocking) -> обратно в payments; финальные неудачи ->
payments.dlx -> payments.new.dlq. Все очереди quorum: dead-lettering at-least-once и x-delivery-limit.
"""

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas.queue import QuorumQueueArgs

from app.config import Settings
from app.messaging.retry import RetryPolicy

# Передоставки без ack (consumer падает посреди обработки) до отправки в DLQ. Штатные retry счётчик не тратят.
DELIVERY_LIMIT = 10

_AT_LEAST_ONCE_DEAD_LETTERING: QuorumQueueArgs = {
    "x-dead-letter-strategy": "at-least-once",
    "x-overflow": "reject-publish",  # обязательное условие at-least-once dead-lettering
}


@dataclass(frozen=True, slots=True)
class Topology:
    exchange: RabbitExchange
    queue: RabbitQueue
    retry_exchange: RabbitExchange
    retry_queues: tuple[RabbitQueue, ...]  # retry_queues[i] — задержка после (i + 1)-й неудачной попытки
    dead_letter_exchange: RabbitExchange
    dead_letter_queue: RabbitQueue

    @property
    def routing_key(self) -> str:
        return self.queue.routing()

    @property
    def dead_letter_routing_key(self) -> str:
        return self.dead_letter_queue.routing()

    def retry_queue_for(self, failed_attempt: int) -> RabbitQueue:
        return self.retry_queues[min(failed_attempt, len(self.retry_queues)) - 1]


def _quorum_queue(name: str, arguments: QuorumQueueArgs | None = None) -> RabbitQueue:
    return RabbitQueue(name, queue_type=QueueType.QUORUM, durable=True, routing_key=name, arguments=arguments)


def build_topology(settings: Settings, retry_policy: RetryPolicy) -> Topology:
    exchange_name = settings.rabbitmq_exchange
    queue_name = settings.rabbitmq_queue
    dlx_name = f"{exchange_name}.dlx"
    dlq_name = f"{queue_name}.dlq"

    queue = _quorum_queue(
        queue_name,
        {
            "x-dead-letter-exchange": dlx_name,
            "x-dead-letter-routing-key": dlq_name,
            "x-delivery-limit": DELIVERY_LIMIT,
            **_AT_LEAST_ONCE_DEAD_LETTERING,
        },
    )
    retry_queues = tuple(
        _quorum_queue(
            f"{queue_name}.retry.{delay_ms}ms",
            {
                "x-message-ttl": delay_ms,
                "x-dead-letter-exchange": exchange_name,
                "x-dead-letter-routing-key": queue_name,
                **_AT_LEAST_ONCE_DEAD_LETTERING,
            },
        )
        for delay_ms in retry_policy.delays_ms()
    )

    return Topology(
        exchange=RabbitExchange(exchange_name, type=ExchangeType.DIRECT, durable=True),
        queue=queue,
        retry_exchange=RabbitExchange(f"{exchange_name}.retry", type=ExchangeType.DIRECT, durable=True),
        retry_queues=retry_queues,
        dead_letter_exchange=RabbitExchange(dlx_name, type=ExchangeType.DIRECT, durable=True),
        # В RabbitMQ 4.x лимит по умолчанию — 20, после чего сообщение удаляется; для DLQ отключаем.
        dead_letter_queue=_quorum_queue(dlq_name, {"x-delivery-limit": -1}),
    )


async def declare_topology(broker: RabbitBroker, topology: Topology, *, include_main_queue: bool = True) -> None:
    """Идемпотентно объявляет топологию. Consumer объявляет payments.new сам — на канале подписчика."""
    exchange = await broker.declare_exchange(topology.exchange)
    retry_exchange = await broker.declare_exchange(topology.retry_exchange)
    dlx = await broker.declare_exchange(topology.dead_letter_exchange)

    if include_main_queue:
        queue = await broker.declare_queue(topology.queue)
        await queue.bind(exchange, routing_key=topology.queue.routing())

    dlq = await broker.declare_queue(topology.dead_letter_queue)
    await dlq.bind(dlx, routing_key=topology.dead_letter_queue.routing())

    for retry_queue in topology.retry_queues:
        declared = await broker.declare_queue(retry_queue)
        await declared.bind(retry_exchange, routing_key=retry_queue.routing())

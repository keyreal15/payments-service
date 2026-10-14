import logging
from typing import Protocol

from faststream.rabbit import RabbitBroker

from app.db.models import OutboxMessage
from app.messaging.broker import PUBLISH_TIMEOUT
from app.messaging.topology import Topology, declare_topology

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    async def ensure_ready(self) -> None:
        """Подключиться к брокеру (идемпотентно). Исключение — брокер недоступен."""
        ...

    async def publish(self, message: OutboxMessage) -> None: ...


class RabbitEventPublisher:
    def __init__(self, broker: RabbitBroker, topology: Topology, timeout: float = PUBLISH_TIMEOUT) -> None:
        self._broker = broker
        self._topology = topology
        self._timeout = timeout
        self._ready = False

    async def ensure_ready(self) -> None:
        if self._ready:
            return  # дальше переподключается robust-соединение aio-pika
        await self._broker.start()
        await declare_topology(self._broker, self._topology)
        self._ready = True
        logger.info("Connected to RabbitMQ, topology declared")

    async def publish(self, message: OutboxMessage) -> None:
        await self._broker.publish(
            message.payload,
            exchange=self._topology.exchange,
            routing_key=message.routing_key,
            message_id=str(message.id),
            correlation_id=str(message.aggregate_id),
            message_type=message.event_type,
            persist=True,
            mandatory=True,
            timeout=self._timeout,
        )

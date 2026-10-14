import asyncio
import uuid
from types import SimpleNamespace
from typing import Any, cast

import pytest
from aiormq.exceptions import DeliveryError
from faststream.rabbit import RabbitBroker
from sqlalchemy import select, text

from app.config import Settings
from app.db.models import OutboxMessage
from app.db.session import SessionFactory
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.publisher import RabbitEventPublisher
from app.messaging.topology import Topology
from app.repositories.outbox import OutboxRepository
from tests.integration.helpers import queue_has, wait_for

BACKOFF_BASE = 30.0


class RecordingPublisher:
    """Фейковый EventPublisher: ошибки можно задать для всех сообщений или для конкретных routing key."""

    def __init__(
        self,
        *,
        fail_all: Exception | None = None,
        fail_routing_keys: frozenset[str] = frozenset(),
        ready_failures: int = 0,
        delay: float = 0.0,
    ) -> None:
        self.published: list[uuid.UUID] = []
        self.ready_calls = 0
        self.fail_all = fail_all
        self._fail_routing_keys = fail_routing_keys
        self._ready_failures = ready_failures
        self._delay = delay

    async def ensure_ready(self) -> None:
        self.ready_calls += 1
        if self._ready_failures > 0:
            self._ready_failures -= 1
            raise ConnectionRefusedError("broker is down")

    async def publish(self, message: OutboxMessage) -> None:
        await asyncio.sleep(self._delay)
        if self.fail_all is not None:
            raise self.fail_all
        if message.routing_key in self._fail_routing_keys:
            raise DeliveryError(None, cast(Any, SimpleNamespace(name="Basic.Return")))
        self.published.append(message.id)


def make_relay(
    session_factory: SessionFactory, publisher: RecordingPublisher | RabbitEventPublisher, batch_size: int = 100
) -> OutboxRelay:
    return OutboxRelay(
        session_factory,
        publisher,
        batch_size=batch_size,
        poll_interval=0.05,
        retry_base_delay=BACKOFF_BASE,
        retry_max_delay=BACKOFF_BASE * 2,
    )


async def add_events(session_factory: SessionFactory, n: int, routing_key: str = "payments.new") -> list[uuid.UUID]:
    ids = [uuid.uuid4() for _ in range(n)]
    async with session_factory() as session, session.begin():
        repo = OutboxRepository(session)
        for payment_id in ids:
            repo.add(
                event_type="payment.created",
                aggregate_id=payment_id,
                routing_key=routing_key,
                payload={"payment_id": str(payment_id)},
            )
    return ids


async def all_events(session_factory: SessionFactory) -> list[OutboxMessage]:
    async with session_factory() as session:
        return list((await session.scalars(select(OutboxMessage).order_by(OutboxMessage.created_at))).all())


async def test_relay_publishes_and_marks_events(session_factory: SessionFactory) -> None:
    await add_events(session_factory, 3)
    publisher = RecordingPublisher()

    result = await make_relay(session_factory, publisher).publish_batch()

    assert (result.published, result.failed, result.broker_unavailable) == (3, 0, False)
    events = await all_events(session_factory)
    assert sorted(publisher.published) == sorted(e.id for e in events)
    assert all(e.published_at is not None for e in events)
    assert (await make_relay(session_factory, publisher).publish_batch()).published == 0


async def test_relay_respects_batch_size(session_factory: SessionFactory) -> None:
    await add_events(session_factory, 5)
    relay = make_relay(session_factory, RecordingPublisher(), batch_size=2)
    assert [(await relay.publish_batch()).published for _ in range(4)] == [2, 2, 1, 0]


NEXT_ATTEMPT_QUERY = text(
    "SELECT aggregate_id, attempts, last_error, published_at, extract(epoch FROM next_attempt_at - now()) FROM outbox"
)


async def test_broker_outage_does_not_postpone_events(session_factory: SessionFactory) -> None:
    await add_events(session_factory, 2)

    result = await make_relay(session_factory, RecordingPublisher(fail_all=ConnectionError("down"))).publish_batch()

    assert (result.published, result.failed, result.broker_unavailable) == (0, 2, True)
    async with session_factory() as session:
        rows = (await session.execute(NEXT_ATTEMPT_QUERY)).all()
    for _, attempts, last_error, published_at, delay in rows:
        assert attempts == 1
        assert "down" in last_error
        assert published_at is None
        assert float(delay) <= 0  # паузу держит цикл relay, сами события не откладываются
    # Брокер вернулся — весь backlog уходит сразу, без ожидания per-message backoff.
    assert (await make_relay(session_factory, RecordingPublisher()).publish_batch()).published == 2


async def test_poison_event_does_not_block_others(session_factory: SessionFactory) -> None:
    [poison] = await add_events(session_factory, 1, routing_key="unroutable")
    healthy = await add_events(session_factory, 3)
    publisher = RecordingPublisher(fail_routing_keys=frozenset({"unroutable"}))

    result = await make_relay(session_factory, publisher).publish_batch()

    # Отказ по конкретному сообщению — не недоступность брокера: остальная пачка опубликована,
    # цикл relay не уходит в общий backoff.
    assert (result.published, result.failed, result.broker_unavailable) == (3, 1, False)
    events = {e.aggregate_id: e for e in await all_events(session_factory)}
    assert events[poison].published_at is None
    assert events[poison].attempts == 1
    assert all(events[h].published_at is not None for h in healthy)
    # Отложено только «ядовитое» сообщение, на свой backoff (считаем по часам БД).
    async with session_factory() as session:
        delays = {row[0]: float(row[4]) for row in (await session.execute(NEXT_ATTEMPT_QUERY)).all()}
    assert BACKOFF_BASE - 5 < delays[poison] <= BACKOFF_BASE


async def test_concurrent_relays_do_not_publish_twice(session_factory: SessionFactory) -> None:
    await add_events(session_factory, 20)
    publisher = RecordingPublisher(delay=0.01)
    relays = [make_relay(session_factory, publisher, batch_size=5) for _ in range(4)]

    for _ in range(5):
        await asyncio.gather(*(relay.publish_batch() for relay in relays))

    assert len(publisher.published) == 20
    assert len(set(publisher.published)) == 20


async def test_run_loop_waits_for_broker_and_wakes_up_on_notify(session_factory: SessionFactory) -> None:
    publisher = RecordingPublisher(ready_failures=2)
    relay = OutboxRelay(
        session_factory, publisher, batch_size=10, poll_interval=60, retry_base_delay=0.05, retry_max_delay=0.1
    )
    task = asyncio.create_task(relay.run())
    try:
        await asyncio.sleep(0.5)  # брокер «поднялся», relay уснул на poll_interval=60 с
        [payment_id] = await add_events(session_factory, 1)
        relay.notify()
        await wait_for(lambda: bool(publisher.published), seconds=5, message="relay woke up on notify")
    finally:
        relay.stop()
        await asyncio.wait_for(task, timeout=5)  # мягкая остановка без cancel
    [event] = await all_events(session_factory)
    assert publisher.ready_calls >= 3  # две неудачные попытки подключения + успешная
    assert publisher.published == [event.id]
    assert event.aggregate_id == payment_id


async def test_relay_publishes_to_rabbitmq(
    session_factory: SessionFactory, publisher_broker: RabbitBroker, topology: Topology, settings: Settings
) -> None:
    await add_events(session_factory, 2, routing_key=topology.routing_key)
    relay = make_relay(session_factory, RabbitEventPublisher(publisher_broker, topology, timeout=5))

    assert (await relay.publish_batch()).published == 2
    await wait_for(queue_has(settings, topology.queue.name, 2), message="messages in payments.new")


async def test_unroutable_message_is_not_marked_published(
    session_factory: SessionFactory, publisher_broker: RabbitBroker, topology: Topology
) -> None:
    await add_events(session_factory, 1, routing_key="nowhere")
    relay = make_relay(session_factory, RabbitEventPublisher(publisher_broker, topology, timeout=5))

    result = await relay.publish_batch()

    assert (result.failed, result.broker_unavailable) == (1, False)
    [event] = await all_events(session_factory)
    assert event.published_at is None
    assert event.attempts == 1


@pytest.mark.parametrize("batch_size", [1, 50])
async def test_batches_with_real_broker(
    session_factory: SessionFactory, publisher_broker: RabbitBroker, topology: Topology, batch_size: int
) -> None:
    await add_events(session_factory, 7, routing_key=topology.routing_key)
    relay = make_relay(session_factory, RabbitEventPublisher(publisher_broker, topology, timeout=5), batch_size)
    published = 0
    while (result := await relay.publish_batch()).published:
        published += result.published
    assert published == 7

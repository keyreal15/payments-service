"""Цикл OutboxRelay без БД: брокер недоступен с самого старта (до чтения outbox дело не доходит)."""

import asyncio
from typing import Any, cast

from app.db.models import OutboxMessage
from app.messaging.outbox_relay import OutboxRelay


class UnreachableBroker:
    def __init__(self) -> None:
        self.connect_attempts = 0

    async def ensure_ready(self) -> None:
        self.connect_attempts += 1
        raise ConnectionRefusedError("broker is down")

    async def publish(self, message: OutboxMessage) -> None:  # не вызывается: брокер недоступен
        raise AssertionError


def make_relay(publisher: UnreachableBroker, base: float = 0.01, max_delay: float = 0.05) -> OutboxRelay:
    return OutboxRelay(
        cast(Any, None), publisher, batch_size=10, poll_interval=1.0, retry_base_delay=base, retry_max_delay=max_delay
    )


def test_backoff_is_exponential_capped_and_never_overflows() -> None:
    relay = make_relay(UnreachableBroker(), base=1.0, max_delay=30.0)
    assert [relay._backoff(n) for n in (1, 2, 3, 4, 5, 6)] == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]
    assert relay._backoff(5000) == 30.0  # 2**4999 переполнил бы float


async def test_relay_survives_broker_outage_and_ignores_notify_storm() -> None:
    broker = UnreachableBroker()
    relay = make_relay(broker, base=0.05, max_delay=0.2)
    task = asyncio.create_task(relay.run())

    for _ in range(200):  # поток новых платежей, пока брокер недоступен
        relay.notify()
        await asyncio.sleep(0.005)

    assert not task.done()
    # Пауза backoff не прерывается notify(): ~1 с при backoff 0.05–0.2 с — единицы попыток, а не 200.
    assert broker.connect_attempts < 20

    relay.stop()
    await asyncio.wait_for(task, timeout=1)  # мягкая остановка прерывает backoff
    assert task.exception() is None

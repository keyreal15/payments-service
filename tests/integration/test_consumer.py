import asyncio
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, cast

import pytest
from faststream.rabbit import RabbitBroker

from app.config import Settings
from app.db.session import SessionFactory
from app.enums import PaymentStatus
from app.messaging.retry import ATTEMPT_HEADER, DEAD_LETTER_REASON_HEADER, LAST_ERROR_HEADER
from app.messaging.topology import Topology
from tests.integration.helpers import (
    ConsumerRunner,
    CountingGateway,
    WebhookRecorder,
    create_pending_payment,
    get_message,
    get_payment,
    queue_has,
    queue_size,
    wait_for,
)


async def publish_event(broker: RabbitBroker, topology: Topology, payment_id: uuid.UUID | str) -> None:
    await broker.publish(
        {"payment_id": str(payment_id)},
        exchange=topology.exchange,
        routing_key=topology.routing_key,
        persist=True,
    )


def dlq_has(settings: Settings, topology: Topology, expected: int) -> Callable[[], Awaitable[bool]]:
    return queue_has(settings, topology.dead_letter_queue.name, expected)


async def test_failed_payment_is_saved_and_webhook_sent(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder()
    gateway = CountingGateway(PaymentStatus.FAILED, delay=1.0)

    async with run_consumer(webhook, gateway):
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(lambda: len(webhook.requests) == 1, message="webhook delivered")

    stored = await get_payment(session_factory, payment.id)
    assert stored.status is PaymentStatus.FAILED
    assert gateway.calls == [str(payment.id)]  # payment.id — idempotency key шлюза
    # processed_at — момент фиксации результата, а не начала транзакции (до вызова шлюза, 1 с).
    assert stored.processed_at is not None
    assert stored.processed_at - stored.created_at >= timedelta(seconds=1)

    body = json.loads(webhook.requests[0].content)
    assert body["event"] == "payment.failed"
    assert body["payment"]["payment_id"] == str(payment.id)
    assert body["payment"]["status"] == "failed"
    assert await queue_size(settings, topology.dead_letter_queue.name) == 0


async def test_webhook_is_retried_with_exponential_backoff_until_success(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder(responder=lambda n: 503 if n < 3 else 200)
    gateway = CountingGateway()

    async with run_consumer(webhook, gateway):
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(lambda: len(webhook.requests) == 3, message="third webhook attempt")

    # Задержки между попытками: base (0.2 с), затем base * 2 (0.4 с).
    first_gap, second_gap = (b - a for a, b in zip(webhook.times, webhook.times[1:], strict=False))
    assert first_gap >= settings.retry_base_delay * 0.9
    assert second_gap >= settings.retry_base_delay * 2 * 0.9
    # Шлюз вызывается один раз: при повторе статус уже финальный, повторяется только webhook.
    assert gateway.calls == [str(payment.id)]
    assert (await get_payment(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED
    assert await queue_size(settings, topology.dead_letter_queue.name) == 0


async def test_gateway_outage_is_retried_and_payment_charged_once(
    run_consumer: ConsumerRunner, publisher_broker: RabbitBroker, topology: Topology, session_factory: SessionFactory
) -> None:
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder()
    gateway = CountingGateway(failures=1)  # первый вызов — технический сбой шлюза

    async with run_consumer(webhook, gateway):
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(lambda: len(webhook.requests) == 1, message="webhook after gateway recovery")

    assert gateway.calls == [str(payment.id), str(payment.id)]  # сбой + успешный повтор
    assert (await get_payment(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED
    assert len(webhook.requests) == 1


async def test_message_goes_to_dlq_after_max_attempts_with_final_error(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder(responder=lambda n: 500 + n)  # 501, 502, 503 — видно, какая ошибка последняя

    async with run_consumer(webhook):
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(dlq_has(settings, topology, 1), seconds=15, message="dead-lettered after 3 attempts")

    assert len(webhook.requests) == settings.retry_max_attempts
    # Статус платежа не зависит от доставки webhook.
    assert (await get_payment(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED

    dead = await get_message(settings, topology.dead_letter_queue.name)
    assert json.loads(dead.body) == {"payment_id": str(payment.id)}
    assert dead.headers[ATTEMPT_HEADER] == settings.retry_max_attempts
    assert dead.headers[DEAD_LETTER_REASON_HEADER] == "max_attempts_exceeded"
    assert "503" in str(dead.headers[LAST_ERROR_HEADER])


async def test_unknown_payment_goes_straight_to_dlq(
    run_consumer: ConsumerRunner, publisher_broker: RabbitBroker, topology: Topology, settings: Settings
) -> None:
    webhook = WebhookRecorder()
    gateway = CountingGateway()

    async with run_consumer(webhook, gateway):
        await publish_event(publisher_broker, topology, uuid.uuid4())
        await wait_for(dlq_has(settings, topology, 1), message="unknown payment dead-lettered")

    dead = await get_message(settings, topology.dead_letter_queue.name)
    assert dead.headers[ATTEMPT_HEADER] == 1  # без retry
    assert dead.headers[DEAD_LETTER_REASON_HEADER] == "permanent_error"
    assert webhook.requests == []
    assert gateway.calls == []


async def test_malformed_message_is_rejected_to_dlq(
    run_consumer: ConsumerRunner, publisher_broker: RabbitBroker, topology: Topology, settings: Settings
) -> None:
    async with run_consumer(WebhookRecorder()):
        await publish_event(publisher_broker, topology, "not-a-uuid")
        await wait_for(dlq_has(settings, topology, 1), message="malformed message dead-lettered")

    dead = await get_message(settings, topology.dead_letter_queue.name)
    # Нативный dead-lettering RabbitMQ (reject): одна запись x-death, retry не было.
    assert ATTEMPT_HEADER not in dead.headers
    deaths = cast(list[dict[str, Any]], dead.headers["x-death"])
    assert [death["reason"] for death in deaths] == ["rejected"]


async def test_sequential_duplicate_does_not_charge_again(
    run_consumer: ConsumerRunner, publisher_broker: RabbitBroker, topology: Topology, session_factory: SessionFactory
) -> None:
    """Outbox даёт at-least-once: дубль после фиксации результата шлюз не вызывает, только повторяет webhook."""
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder()
    gateway = CountingGateway()

    async with run_consumer(webhook, gateway, consumer_prefetch_count=1):
        await publish_event(publisher_broker, topology, payment.id)
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(lambda: len(webhook.requests) == 2, message="both duplicates handled")

    assert gateway.calls == [str(payment.id)]
    assert [json.loads(r.content)["payment"]["status"] for r in webhook.requests] == ["succeeded", "succeeded"]


async def test_concurrent_duplicate_is_deferred_while_payment_is_locked(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Параллельный дубль видит строку под блокировкой: шлюз второй раз не вызывается, а сообщение
    не теряется — откладывается и после разблокировки доставляет webhook с финальным статусом."""
    caplog.set_level(logging.INFO, logger="app.consumer.handler")
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder()
    gateway = CountingGateway(delay=0.5)

    async with run_consumer(webhook, gateway, consumer_prefetch_count=10):
        await publish_event(publisher_broker, topology, payment.id)
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(lambda: len(webhook.requests) == 2, message="owner and deferred duplicate")

    assert gateway.calls == [str(payment.id)]
    assert {json.loads(r.content)["payment"]["status"] for r in webhook.requests} == {"succeeded"}
    assert await queue_size(settings, topology.dead_letter_queue.name) == 0
    # Дубль действительно застал блокировку и был отложен (а не просто пришёл после коммита).
    deferrals = [r for r in caplog.records if "locked by another consumer" in r.getMessage()]
    assert deferrals
    assert all("deferring" in r.getMessage() for r in deferrals)


@pytest.mark.parametrize("prefetch", [1, 3])
async def test_prefetch_count_limits_concurrency(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    prefetch: int,
) -> None:
    payments = [await create_pending_payment(session_factory) for _ in range(4)]
    webhook = WebhookRecorder()
    gateway = CountingGateway(delay=0.3)

    async with run_consumer(webhook, gateway, consumer_prefetch_count=prefetch):
        for payment in payments:
            await publish_event(publisher_broker, topology, payment.id)
        await wait_for(lambda: len(webhook.requests) == len(payments), message="all payments processed")

    if prefetch == 1:
        assert gateway.max_in_flight == 1
    else:
        assert 1 < gateway.max_in_flight <= prefetch


async def test_single_attempt_policy_dead_letters_immediately(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder(responder=lambda _: 500)

    async with run_consumer(webhook, retry_max_attempts=1):
        await publish_event(publisher_broker, topology, payment.id)
        await wait_for(dlq_has(settings, topology, 1), message="dead-lettered on first failure")
        await asyncio.sleep(0.5)  # убедиться, что повторов не будет

    assert len(webhook.requests) == 1

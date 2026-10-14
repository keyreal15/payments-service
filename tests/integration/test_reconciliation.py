import json
import uuid

from faststream.rabbit import RabbitBroker
from sqlalchemy import select, text

from app.config import Settings
from app.db.models import OutboxMessage
from app.db.session import SessionFactory
from app.enums import PaymentStatus
from app.messaging.topology import Topology
from app.repositories.outbox import OutboxRepository
from app.services.payments import PAYMENT_CREATED_EVENT
from app.services.reconciliation import PAYMENT_RECONCILIATION_EVENT, StalePaymentsReconciler
from tests.integration.helpers import (
    ConsumerRunner,
    CountingGateway,
    WebhookRecorder,
    api_client_for,
    create_pending_payment,
    get_payment,
    queue_has,
    wait_for,
)

STALE_AFTER = 60.0


def make_reconciler(session_factory: SessionFactory, notified: list[int] | None = None) -> StalePaymentsReconciler:
    return StalePaymentsReconciler(
        session_factory,
        routing_key="payments.new",
        stale_after=STALE_AFTER,
        interval=0.1,
        on_enqueued=(lambda: notified.append(1)) if notified is not None else None,
    )


async def age(session_factory: SessionFactory, payment_id: uuid.UUID, seconds: float) -> None:
    """Сдвигает в прошлое создание платежа и его событий (вместо ожидания в тесте)."""
    async with session_factory() as session, session.begin():
        params = {"id": payment_id, "seconds": seconds}
        shift = "now() - make_interval(secs => :seconds)"
        await session.execute(text(f"UPDATE payments SET created_at = {shift} WHERE id = :id"), params)  # noqa: S608
        await session.execute(text(f"UPDATE outbox SET created_at = {shift} WHERE aggregate_id = :id"), params)  # noqa: S608


async def events_for(session_factory: SessionFactory, payment_id: uuid.UUID) -> list[OutboxMessage]:
    async with session_factory() as session:
        stmt = select(OutboxMessage).where(OutboxMessage.aggregate_id == payment_id).order_by(OutboxMessage.created_at)
        return list((await session.scalars(stmt)).all())


async def add_event(session_factory: SessionFactory, payment_id: uuid.UUID) -> None:
    async with session_factory() as session, session.begin():
        OutboxRepository(session).add(
            event_type=PAYMENT_CREATED_EVENT,
            aggregate_id=payment_id,
            routing_key="payments.new",
            payload={"payment_id": str(payment_id)},
        )


async def test_stale_pending_payment_is_reenqueued_once(session_factory: SessionFactory) -> None:
    payment = await create_pending_payment(session_factory)
    await add_event(session_factory, payment.id)
    await age(session_factory, payment.id, STALE_AFTER * 2)
    notified: list[int] = []
    reconciler = make_reconciler(session_factory, notified)

    assert await reconciler.reconcile_once() == 1
    assert notified == [1]  # relay разбудили сразу
    [original, retry] = await events_for(session_factory, payment.id)
    assert original.event_type == PAYMENT_CREATED_EVENT
    assert retry.event_type == PAYMENT_RECONCILIATION_EVENT
    assert retry.payload == {"payment_id": str(payment.id)}
    # Следующий проход не дублирует событие, пока не пройдёт ещё stale_after.
    assert await reconciler.reconcile_once() == 0


async def test_payments_that_must_not_be_reconciled(session_factory: SessionFactory) -> None:
    fresh = await create_pending_payment(session_factory)

    finalized = await create_pending_payment(session_factory)
    leased = await create_pending_payment(session_factory)
    recently_enqueued = await create_pending_payment(session_factory)
    for payment in (finalized, leased, recently_enqueued):
        await age(session_factory, payment.id, STALE_AFTER * 2)
    await add_event(session_factory, recently_enqueued.id)  # событие свежее, платёж старый
    async with session_factory() as session, session.begin():
        await session.execute(
            text("UPDATE payments SET status = 'succeeded', processed_at = now() WHERE id = :id"), {"id": finalized.id}
        )
        await session.execute(
            text(
                "UPDATE payments SET lease_id = gen_random_uuid(), lease_expires_at = now() + interval '1 minute' "
                "WHERE id = :id"
            ),
            {"id": leased.id},
        )

    assert await make_reconciler(session_factory).reconcile_once() == 0
    for payment in (fresh, finalized, leased):
        assert await events_for(session_factory, payment.id) == []


async def test_payment_dead_lettered_after_gateway_outage_is_completed_by_reconciliation(
    settings: Settings,
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
) -> None:
    """Шлюз трижды не ответил -> сообщение в DLQ, платёж в pending -> сверка -> платёж проведён."""
    webhook = WebhookRecorder()
    gateway = CountingGateway(failures=settings.retry_max_attempts)
    api_settings = settings.model_copy(update={"reconcile_stale_after": 1.0, "reconcile_interval": 0.2})
    body = {"amount": "10.00", "currency": "USD", "webhook_url": "https://merchant.example/hook"}

    async with run_consumer(webhook, gateway), api_client_for(api_settings) as client:
        created = await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": uuid.uuid4().hex})
        payment_id = uuid.UUID(created.json()["payment_id"])
        await wait_for(queue_has(settings, topology.dead_letter_queue.name, 1), message="dead-lettered")

        async def succeeded() -> bool:
            return (await get_payment(session_factory, payment_id)).status is PaymentStatus.SUCCEEDED

        await wait_for(succeeded, seconds=15, message="payment completed after reconciliation")
        await wait_for(lambda: bool(webhook.requests), message="webhook after reconciliation")

    assert set(gateway.calls) == {str(payment_id)}  # все вызовы — с тем же idempotency key
    assert len(gateway.calls) > settings.retry_max_attempts
    assert json.loads(webhook.requests[-1].content)["event"] == "payment.succeeded"
    event_types = [event.event_type for event in await events_for(session_factory, payment_id)]
    assert event_types[0] == PAYMENT_CREATED_EVENT
    assert PAYMENT_RECONCILIATION_EVENT in event_types

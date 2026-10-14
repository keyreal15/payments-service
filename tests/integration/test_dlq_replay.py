import json

from faststream.rabbit import RabbitBroker

from app.config import Settings
from app.db.session import SessionFactory
from app.messaging.dlq_replay import replay
from app.messaging.retry import ATTEMPT_HEADER, DEAD_LETTER_REASON_HEADER, LAST_ERROR_HEADER
from app.messaging.topology import Topology
from tests.integration.helpers import (
    ConsumerRunner,
    WebhookRecorder,
    create_pending_payment,
    get_message,
    queue_has,
    queue_size,
    wait_for,
)


async def put_dead_letters(broker: RabbitBroker, topology: Topology, count: int) -> None:
    for i in range(count):
        await broker.publish(
            {"payment_id": f"dead-{i}"},
            exchange=topology.dead_letter_exchange,
            routing_key=topology.dead_letter_routing_key,
            headers={ATTEMPT_HEADER: 3, DEAD_LETTER_REASON_HEADER: "max_attempts_exceeded", LAST_ERROR_HEADER: "503"},
        )


async def test_replay_returns_dead_letters_to_processing(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    payment = await create_pending_payment(session_factory)
    receiver_fixed = False
    webhook = WebhookRecorder(responder=lambda _: 200 if receiver_fixed else 500)

    async with run_consumer(webhook):
        await publisher_broker.publish(
            {"payment_id": str(payment.id)}, exchange=topology.exchange, routing_key=topology.routing_key
        )
        await wait_for(queue_has(settings, topology.dead_letter_queue.name, 1), seconds=15, message="dead-lettered")
        failed_deliveries = len(webhook.requests)

        receiver_fixed = True  # «клиент починил webhook»
        assert await replay(settings, limit=100) == 1
        await wait_for(lambda: len(webhook.requests) == failed_deliveries + 1, message="replayed webhook")

    assert json.loads(webhook.requests[-1].content)["payment"]["payment_id"] == str(payment.id)
    assert await queue_size(settings, topology.dead_letter_queue.name) == 0


async def test_replay_resets_attempts_and_keeps_failure_details(
    publisher_broker: RabbitBroker, topology: Topology, settings: Settings
) -> None:
    await put_dead_letters(publisher_broker, topology, 1)

    assert await replay(settings, limit=100) == 1

    replayed = await get_message(settings, topology.queue.name)
    assert ATTEMPT_HEADER not in replayed.headers
    assert replayed.headers["x-replayed-dead-letter-reason"] == "max_attempts_exceeded"
    assert replayed.headers["x-replayed-last-error"] == "503"


async def test_replay_respects_limit(publisher_broker: RabbitBroker, topology: Topology, settings: Settings) -> None:
    await put_dead_letters(publisher_broker, topology, 3)

    assert await replay(settings, limit=2) == 2
    assert await queue_size(settings, topology.dead_letter_queue.name) == 1


async def test_replay_does_not_loop_over_messages_that_fail_again(
    run_consumer: ConsumerRunner,
    publisher_broker: RabbitBroker,
    topology: Topology,
    session_factory: SessionFactory,
    settings: Settings,
) -> None:
    """Сообщение, которое сразу снова падает в DLQ, переотправляется один раз, а не до --limit."""
    payment = await create_pending_payment(session_factory)
    webhook = WebhookRecorder(responder=lambda _: 500)

    async with run_consumer(webhook, retry_max_attempts=1):  # DLQ с первой же неудачи
        await publisher_broker.publish(
            {"payment_id": str(payment.id)}, exchange=topology.exchange, routing_key=topology.routing_key
        )
        await wait_for(queue_has(settings, topology.dead_letter_queue.name, 1), message="dead-lettered")

        assert await replay(settings, limit=100) == 1
        await wait_for(queue_has(settings, topology.dead_letter_queue.name, 1), message="dead-lettered again")

    assert len(webhook.requests) == 2

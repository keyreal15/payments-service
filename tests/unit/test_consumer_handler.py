"""Исходы обработки сообщения в PaymentCreatedHandler и маппинг ошибок PaymentProcessor — на фейках."""

import uuid
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, cast

import asyncpg
import pytest
from aiormq.exceptions import DeliveryError, PublishError
from faststream.exceptions import NackMessage, RejectMessage
from pamqp.commands import Basic

from app.consumer.handler import PaymentCreatedHandler
from app.messaging.retry import ATTEMPT_HEADER, DEAD_LETTER_REASON_HEADER, LAST_ERROR_HEADER, RetryPolicy
from app.messaging.topology import build_topology
from app.schemas import PaymentEvent
from app.services.errors import (
    DatabaseUnavailableError,
    PaymentBusyError,
    PaymentNotFoundError,
    WebhookUrlNotAllowedError,
)
from app.services.gateway import GatewayError
from app.services.processing import PaymentProcessor
from app.services.webhooks import WebhookDeliveryError
from tests.settings import make_settings

POLICY = RetryPolicy(max_attempts=3, base_delay=0.01, multiplier=2.0)
TOPOLOGY = build_topology(make_settings(rabbitmq_exchange="payments", rabbitmq_queue="payments.new"), POLICY)


@dataclass
class FakeBroker:
    error: Exception | None = None
    published: list[dict[str, Any]] = field(default_factory=list)

    async def publish(self, body: bytes, **kwargs: Any) -> None:
        if self.error is not None:
            raise self.error
        self.published.append({"body": body, **kwargs})


@dataclass
class FakeProcessor:
    error: Exception | None = None

    async def process(self, payment_id: uuid.UUID) -> None:
        if self.error is not None:
            raise self.error


def make_message(attempt: int | None, headers: dict[str, Any]) -> Any:
    headers = dict(headers)
    if attempt is not None:
        headers[ATTEMPT_HEADER] = attempt
    return SimpleNamespace(
        headers=headers,
        body=b'{"payment_id": "x"}',
        message_id="msg-1",
        correlation_id="corr-1",
        content_type="application/json",
        raw_message=SimpleNamespace(type="payment.created"),
    )


async def handle(
    error: Exception | None,
    attempt: int | None = None,
    broker: FakeBroker | None = None,
    headers: dict[str, Any] | None = None,
) -> FakeBroker:
    broker = broker or FakeBroker()
    handler = PaymentCreatedHandler(
        cast(Any, FakeProcessor(error)), cast(Any, broker), TOPOLOGY, POLICY, publish_timeout=1.0
    )
    await handler.handle(PaymentEvent(payment_id=uuid.uuid4()), make_message(attempt, headers or {}))
    return broker


async def test_success_publishes_nothing() -> None:
    assert (await handle(None)).published == []


@pytest.mark.parametrize(
    "error", [WebhookDeliveryError("HTTP 500"), GatewayError("timeout"), ValueError("unexpected bug")]
)
async def test_failure_is_retried_with_next_attempt_and_exponential_queue(error: Exception) -> None:
    [first] = (await handle(error, attempt=1)).published
    assert first["exchange"] is TOPOLOGY.retry_exchange
    assert first["routing_key"] == "payments.new.retry.10ms"
    assert first["headers"][ATTEMPT_HEADER] == 2
    assert type(error).__name__ in first["headers"][LAST_ERROR_HEADER]
    assert first["message_id"] == "msg-1"
    assert first["persist"] is True

    [second] = (await handle(error, attempt=2)).published
    assert second["routing_key"] == "payments.new.retry.20ms"
    assert second["headers"][ATTEMPT_HEADER] == 3


async def test_exhausted_attempts_go_to_dlq_with_final_error() -> None:
    [dead] = (await handle(WebhookDeliveryError("final 503"), attempt=3)).published
    assert dead["exchange"] is TOPOLOGY.dead_letter_exchange
    assert dead["routing_key"] == "payments.new.dlq"
    assert dead["headers"][ATTEMPT_HEADER] == 3
    assert "final 503" in dead["headers"][LAST_ERROR_HEADER]
    assert dead["headers"][DEAD_LETTER_REASON_HEADER] == "max_attempts_exceeded"


@pytest.mark.parametrize("error", [PaymentNotFoundError(uuid.uuid4()), WebhookUrlNotAllowedError("10.0.0.1")])
async def test_permanent_errors_go_straight_to_dlq(error: Exception) -> None:
    [dead] = (await handle(error)).published
    assert dead["exchange"] is TOPOLOGY.dead_letter_exchange
    assert dead["headers"][ATTEMPT_HEADER] == 1
    assert dead["headers"][DEAD_LETTER_REASON_HEADER] == "permanent_error"


@pytest.mark.parametrize("error", [PaymentBusyError(uuid.uuid4()), DatabaseUnavailableError("db is restarting")])
async def test_transient_errors_do_not_consume_attempts(error: Exception) -> None:
    [deferred] = (await handle(error, attempt=3)).published  # даже на последней попытке — не в DLQ
    assert deferred["exchange"] is TOPOLOGY.retry_exchange
    assert deferred["routing_key"] == "payments.new.retry.10ms"
    assert deferred["headers"][ATTEMPT_HEADER] == 3


async def test_broker_headers_are_dropped_and_custom_headers_preserved() -> None:
    broker = await handle(
        WebhookDeliveryError("503"),
        attempt=1,
        headers={"x-death": [{}], "x-delivery-count": 2, "traceparent": "00-abc"},
    )
    headers = broker.published[0]["headers"]
    assert "x-death" not in headers
    assert "x-delivery-count" not in headers
    assert headers["traceparent"] == "00-abc"


async def test_unroutable_republish_rejects_to_dlx() -> None:
    returned = SimpleNamespace(delivery=Basic.Return(reply_code=312, reply_text="NO_ROUTE"))
    broker = FakeBroker(error=PublishError(cast(Any, returned), cast(Any, returned.delivery)))
    with pytest.raises(RejectMessage):
        await handle(WebhookDeliveryError("503"), attempt=1, broker=broker)


@pytest.mark.parametrize(
    "publish_error",
    [TimeoutError(), DeliveryError(None, cast(Any, Basic.Nack())), ConnectionResetError()],
)
async def test_transient_republish_failure_returns_message_to_queue(publish_error: Exception) -> None:
    with pytest.raises(NackMessage):
        await handle(WebhookDeliveryError("503"), attempt=1, broker=FakeBroker(error=publish_error))


@pytest.mark.parametrize(
    "db_error",
    [
        asyncpg.CannotConnectNowError("the database system is starting up"),
        asyncpg.TooManyConnectionsError("too many connections"),
        TimeoutError(),
    ],
)
async def test_processor_maps_database_unavailability(db_error: Exception) -> None:
    def broken_session_factory() -> Any:
        raise db_error

    processor = PaymentProcessor(
        cast(Any, broken_session_factory), cast(Any, None), cast(Any, None), gateway_timeout=1.0
    )
    with pytest.raises(DatabaseUnavailableError):
        await processor.process(uuid.uuid4())


async def test_processor_keeps_logic_errors_as_is() -> None:
    def broken_session_factory() -> Any:
        raise ValueError("bug")

    processor = PaymentProcessor(
        cast(Any, broken_session_factory), cast(Any, None), cast(Any, None), gateway_timeout=1.0
    )
    with pytest.raises(ValueError, match="bug"):
        await processor.process(uuid.uuid4())

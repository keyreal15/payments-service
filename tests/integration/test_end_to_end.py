import json
import uuid

import pytest
from sqlalchemy import select

from app.config import Settings
from app.db.models import OutboxMessage
from app.db.session import SessionFactory
from tests.integration.helpers import ConsumerRunner, WebhookRecorder, api_client_for, wait_for


@pytest.mark.parametrize("amount", ["99.90", 99.9])
async def test_full_flow_api_outbox_rabbitmq_consumer_webhook(
    settings: Settings, run_consumer: ConsumerRunner, session_factory: SessionFactory, amount: str | float
) -> None:
    """POST /payments -> outbox -> relay -> RabbitMQ -> consumer -> БД -> webhook -> GET /payments/{id}."""
    webhook = WebhookRecorder()
    body = {
        "amount": amount,
        "currency": "EUR",
        "description": "e2e",
        "metadata": {"order": "A-1"},
        "webhook_url": "https://merchant.example/hook",
    }

    async with run_consumer(webhook), api_client_for(settings) as client:  # relay включён
        created = await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": uuid.uuid4().hex})
        assert created.status_code == 202
        await wait_for(lambda: len(webhook.requests) == 1, message="webhook after full flow")
        payment = (await client.get(created.headers["Location"])).json()

    assert payment["status"] == "succeeded"
    assert payment["amount"] == "99.90"
    assert payment["processed_at"] is not None
    assert json.loads(webhook.requests[0].content) == {"event": "payment.succeeded", "payment": payment}

    async with session_factory() as session:
        [event] = (await session.scalars(select(OutboxMessage))).all()
    assert event.published_at is not None

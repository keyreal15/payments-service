import asyncio
import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import func, select

from app.api.app import create_app
from app.api.dependencies import AppContainer
from app.config import Settings
from app.db.models import OutboxMessage, Payment
from app.db.session import SessionFactory, create_db_engine, create_session_factory
from app.messaging.topology import Topology
from app.services.payments import PAYMENT_CREATED_EVENT
from tests.integration.helpers import api_client_for

BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42, "customer": {"tier": "gold"}},
    "webhook_url": "https://merchant.example/webhooks",
}


def key() -> dict[str, str]:
    return {"Idempotency-Key": uuid.uuid4().hex}


async def count(session_factory: SessionFactory, model: type[Payment] | type[OutboxMessage]) -> int:
    async with session_factory() as session:
        return (await session.scalar(select(func.count()).select_from(model))) or 0


# --- Аутентификация и защита ---


@pytest.mark.parametrize("api_key", [None, "wrong", ""])
async def test_create_requires_valid_api_key(
    api_client: httpx.AsyncClient, session_factory: SessionFactory, api_key: str | None
) -> None:
    api_client.headers.pop("X-API-Key")
    headers = key() if api_key is None else {**key(), "X-API-Key": api_key}

    response = await api_client.post("/api/v1/payments", json=BODY, headers=headers)

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key", "code": "unauthorized"}
    assert response.headers["WWW-Authenticate"] == "APIKey"
    assert await count(session_factory, Payment) == 0


async def test_get_requires_api_key(api_client: httpx.AsyncClient) -> None:
    api_client.headers.pop("X-API-Key")
    assert (await api_client.get(f"/api/v1/payments/{uuid.uuid4()}")).status_code == 401


@pytest.mark.parametrize(
    "content", [b'{"amount": -1}', b"{broken json", b"x" * 100_000], ids=["invalid", "malformed", "huge"]
)
async def test_auth_is_checked_before_body_is_parsed(api_client: httpx.AsyncClient, content: bytes) -> None:
    api_client.headers.pop("X-API-Key")
    response = await api_client.post(
        "/api/v1/payments", content=content, headers={**key(), "Content-Type": "application/json"}
    )
    assert response.status_code == 401


async def test_body_size_is_limited(api_client: httpx.AsyncClient) -> None:
    body = {**BODY, "description": "x" * 100_000}
    response = await api_client.post("/api/v1/payments", json=body, headers=key())
    assert response.status_code == 413
    assert response.json()["code"] == "payload_too_large"


async def test_health_endpoints_are_public(api_client: httpx.AsyncClient, session_factory: SessionFactory) -> None:
    api_client.headers.pop("X-API-Key")
    assert (await api_client.get("/health/live")).json() == {"status": "ok"}
    ready = await api_client.get("/health/ready")
    assert ready.status_code == 200
    # В фикстуре relay выключен — API к брокеру не подключается.
    assert ready.json() == {
        "status": "ok",
        "database": "ok",
        "rabbitmq": "disabled",
        "outbox_relay": "disabled",
        "outbox_backlog": 0,
    }


async def test_readiness_fails_without_database(settings: Settings) -> None:
    app = create_app(settings.model_copy(update={"outbox_relay_enabled": False}))
    broken = create_db_engine(
        settings.model_copy(update={"database_url": "postgresql+asyncpg://nobody:x@127.0.0.1:1/none"}),
        application_name="test",
    )
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        container: AppContainer = app.state.container
        container.session_factory = create_session_factory(broken)
        response = await client.get("/health/ready")
    await broken.dispose()

    assert response.status_code == 503
    assert response.json()["database"] == "unavailable"


async def test_readiness_fails_when_outbox_relay_crashed(settings: Settings) -> None:
    async def crash() -> None:
        raise RuntimeError("relay bug")

    app = create_app(settings.model_copy(update={"outbox_relay_enabled": False}))
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        crashed = asyncio.create_task(crash())
        await asyncio.wait([crashed])
        container: AppContainer = app.state.container
        container.relay_task = crashed
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["outbox_relay"] == "stopped"


async def test_api_accepts_payments_while_rabbitmq_is_unreachable(
    settings: Settings, session_factory: SessionFactory
) -> None:
    """Смысл outbox: API стартует и принимает платежи без брокера, события ждут в outbox."""
    unreachable = settings.model_copy(update={"rabbitmq_url": "amqp://guest:guest@127.0.0.1:1/"})
    async with api_client_for(unreachable) as client:
        response = await client.post("/api/v1/payments", json=BODY, headers=key())
        ready = await client.get("/health/ready")

    assert response.status_code == 202
    assert ready.status_code == 200
    assert ready.json()["rabbitmq"] == "unavailable"
    assert ready.json()["outbox_relay"] == "running"
    assert ready.json()["outbox_backlog"] == 1
    async with session_factory() as session:
        [event] = (await session.scalars(select(OutboxMessage))).all()
    assert event.published_at is None


async def test_unknown_route_uses_common_error_format(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/api/v1/unknown")
    assert response.status_code == 404
    assert response.json()["code"] == "not_found"


# --- Создание платежа ---


async def test_create_payment_returns_202_and_writes_outbox(
    api_client: httpx.AsyncClient, session_factory: SessionFactory, topology: Topology
) -> None:
    headers = key()
    response = await api_client.post("/api/v1/payments", json=BODY, headers=headers)

    assert response.status_code == 202
    assert response.headers["Idempotent-Replayed"] == "false"
    data = response.json()
    assert set(data) == {"payment_id", "status", "created_at"}
    assert data["status"] == "pending"
    assert response.headers["Location"] == f"/api/v1/payments/{data['payment_id']}"

    async with session_factory() as session:
        payment = await session.get(Payment, uuid.UUID(data["payment_id"]))
        outbox = (await session.scalars(select(OutboxMessage))).all()

    assert payment is not None
    assert str(payment.amount) == "1500.00"
    assert payment.meta == BODY["metadata"]
    assert payment.idempotency_key == headers["Idempotency-Key"]
    assert payment.processed_at is None

    [event] = outbox
    assert event.event_type == PAYMENT_CREATED_EVENT
    assert event.aggregate_id == payment.id
    assert event.routing_key == topology.routing_key
    assert event.payload == {"payment_id": str(payment.id)}
    assert event.published_at is None


async def test_amount_is_normalized_to_two_decimals(api_client: httpx.AsyncClient) -> None:
    created = await api_client.post("/api/v1/payments", json={**BODY, "amount": 100}, headers=key())
    payment = await api_client.get(f"/api/v1/payments/{created.json()['payment_id']}")
    assert payment.json()["amount"] == "100.00"


@pytest.mark.parametrize(
    "body",
    [
        {**BODY, "amount": "-1"},
        {**BODY, "amount": "0"},
        {**BODY, "amount": "1.001"},
        {**BODY, "amount": 99999999999999.99},  # float уже потерял точность
        {**BODY, "currency": "GBP"},
        {**BODY, "webhook_url": "not-a-url"},
        {**BODY, "webhook_url": "http://a.com/" + "я" * 400},
        {**BODY, "metadata": "string"},
        {**BODY, "description": "a\u0000b"},
        {**BODY, "metadata": {"x": "\u0000"}},
        {**BODY, "unexpected": True},
        {k: v for k, v in BODY.items() if k != "amount"},
    ],
)
async def test_create_payment_validation(
    api_client: httpx.AsyncClient, session_factory: SessionFactory, body: dict[str, Any]
) -> None:
    response = await api_client.post("/api/v1/payments", json=body, headers=key())
    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"
    assert all(set(error) == {"loc", "msg", "type"} for error in response.json()["detail"])
    assert await count(session_factory, Payment) == 0


@pytest.mark.parametrize(
    "raw",
    [
        b'{"amount": NaN, "currency": "RUB", "webhook_url": "http://x.com"}',
        b'{"amount": "1", "currency": "RUB", "webhook_url": "http://x.com", "metadata": {"x": Infinity}}',
        b'{"amount": "1", "currency": "RUB", "webhook_url": "http://x.com", "description": "\\ud800"}',
    ],
    ids=["nan", "infinity", "lone-surrogate"],
)
async def test_non_standard_json_values_are_422_not_500(api_client: httpx.AsyncClient, raw: bytes) -> None:
    response = await api_client.post(
        "/api/v1/payments", content=raw, headers={**key(), "Content-Type": "application/json"}
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "headers", [{}, {"Idempotency-Key": ""}, {"Idempotency-Key": "x" * 256}, {"Idempotency-Key": "has space"}]
)
async def test_idempotency_key_is_required_and_validated(
    api_client: httpx.AsyncClient, headers: dict[str, str]
) -> None:
    response = await api_client.post("/api/v1/payments", json=BODY, headers=headers)
    assert response.status_code == 422


# --- Идемпотентность ---


async def test_repeated_request_returns_same_payment(
    api_client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    headers = key()
    first = await api_client.post("/api/v1/payments", json=BODY, headers=headers)
    # Тот же запрос в другом представлении: сумма числом, другой порядок ключей metadata.
    same = {**BODY, "amount": 1500, "metadata": {"customer": {"tier": "gold"}, "order_id": 42}}
    second = await api_client.post("/api/v1/payments", json=same, headers=headers)

    assert first.status_code == second.status_code == 202
    assert second.headers["Idempotent-Replayed"] == "true"
    assert first.json() == second.json()
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxMessage) == 1


async def test_same_key_with_different_body_is_rejected(
    api_client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    headers = key()
    await api_client.post("/api/v1/payments", json=BODY, headers=headers)
    response = await api_client.post("/api/v1/payments", json={**BODY, "amount": "1.00"}, headers=headers)

    assert response.status_code == 422
    assert response.json()["code"] == "idempotency_key_reused"
    assert "Idempotency-Key" in response.json()["detail"]
    assert await count(session_factory, Payment) == 1


async def test_concurrent_requests_with_same_key_create_one_payment(
    api_client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    headers = key()
    responses = await asyncio.gather(
        *(api_client.post("/api/v1/payments", json=BODY, headers=headers) for _ in range(10))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert [r.headers["Idempotent-Replayed"] for r in responses].count("false") == 1
    assert await count(session_factory, Payment) == 1
    assert await count(session_factory, OutboxMessage) == 1


async def test_concurrent_requests_with_same_key_and_different_bodies(
    api_client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    headers = key()
    bodies = [{**BODY, "amount": f"{i}.00"} for i in range(1, 6)]
    responses = await asyncio.gather(*(api_client.post("/api/v1/payments", json=b, headers=headers) for b in bodies))

    assert sorted(r.status_code for r in responses) == [202, 422, 422, 422, 422]
    assert await count(session_factory, Payment) == 1


async def test_different_keys_create_different_payments(
    api_client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    first = await api_client.post("/api/v1/payments", json=BODY, headers=key())
    second = await api_client.post("/api/v1/payments", json=BODY, headers=key())
    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await count(session_factory, Payment) == 2


# --- Получение платежа ---


async def test_get_payment(api_client: httpx.AsyncClient) -> None:
    headers = key()
    created = (await api_client.post("/api/v1/payments", json=BODY, headers=headers)).json()

    response = await api_client.get(f"/api/v1/payments/{created['payment_id']}")

    assert response.status_code == 200
    assert response.json() == {
        "payment_id": created["payment_id"],
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Order #42",
        "metadata": BODY["metadata"],
        "status": "pending",
        "idempotency_key": headers["Idempotency-Key"],
        "webhook_url": BODY["webhook_url"],
        "created_at": created["created_at"],
        "processed_at": None,
    }


async def test_get_unknown_payment_returns_404(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Payment not found", "code": "payment_not_found"}


async def test_get_payment_with_invalid_id_returns_422(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/api/v1/payments/not-a-uuid")
    assert response.status_code == 422

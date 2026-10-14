import asyncio
import inspect
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from decimal import Decimal

import aio_pika
import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage

from app.api.app import create_app
from app.config import Settings
from app.db.models import Payment
from app.db.session import SessionFactory
from app.enums import Currency, PaymentStatus
from app.repositories.payments import PaymentRepository
from app.services.gateway import GatewayError
from tests.settings import TEST_API_KEY

ConsumerRunner = Callable[..., AbstractAsyncContextManager[None]]


@asynccontextmanager
async def api_client_for(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """API с полным lifespan (как в проде) и клиентом с валидным X-API-Key."""
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://test", headers={"X-API-Key": TEST_API_KEY}) as client,
    ):
        yield client


class WebhookRecorder:
    """Мок webhook-получателя: запоминает запросы (и время) и отвечает кодом из `responder(номер_запроса)`."""

    def __init__(self, responder: Callable[[int], int] = lambda _: 200) -> None:
        self.requests: list[httpx.Request] = []
        self.times: list[float] = []
        self._responder = responder

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        self.times.append(asyncio.get_running_loop().time())
        return httpx.Response(self._responder(len(self.requests)))


class CountingGateway:
    """Фейковый шлюз: считает вызовы и параллелизм; может падать первые `failures` раз."""

    def __init__(self, result: PaymentStatus = PaymentStatus.SUCCEEDED, delay: float = 0.0, failures: int = 0) -> None:
        self.calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self._result = result
        self._delay = delay
        self._failures = failures

    async def charge(self, *, idempotency_key: str, amount: Decimal, currency: Currency) -> PaymentStatus:
        self.calls.append(idempotency_key)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self._delay)
        finally:
            self.in_flight -= 1
        if len(self.calls) <= self._failures:
            raise GatewayError("emulated outage")
        return self._result


async def create_pending_payment(
    session_factory: SessionFactory, webhook_url: str = "https://merchant.example/hook"
) -> Payment:
    async with session_factory() as session, session.begin():
        payment = await PaymentRepository(session).create_if_absent(
            amount=Decimal("42.00"),
            currency=Currency.RUB,
            description="integration",
            metadata={"source": "tests"},
            webhook_url=webhook_url,
            idempotency_key=uuid.uuid4().hex,
            request_hash="0" * 64,
        )
    assert payment is not None
    return payment


async def get_payment(session_factory: SessionFactory, payment_id: uuid.UUID) -> Payment:
    async with session_factory() as session:
        payment = await PaymentRepository(session).get(payment_id)
    assert payment is not None
    return payment


async def queue_size(settings: Settings, queue_name: str) -> int:
    connection = await aio_pika.connect(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        count = queue.declaration_result.message_count
        assert count is not None
        return count


def queue_has(settings: Settings, queue_name: str, expected: int) -> Callable[[], Awaitable[bool]]:
    """Условие для wait_for: в очереди ровно `expected` готовых сообщений."""

    async def check() -> bool:
        return await queue_size(settings, queue_name) == expected

    return check


async def get_message(settings: Settings, queue_name: str) -> AbstractIncomingMessage:
    connection = await aio_pika.connect(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, passive=True)
        message = await queue.get(no_ack=True, fail=False)
        assert message is not None, f"queue {queue_name} is empty"
        return message


async def wait_for(
    condition: Callable[[], bool | Awaitable[bool]],
    *,
    seconds: float = 10.0,
    interval: float = 0.05,
    message: str = "",
) -> None:
    """Опрос условия: для eventual-consistency проверок (RabbitMQ, фоновые задачи)."""
    try:
        async with asyncio.timeout(seconds):
            while True:
                result = condition()
                if inspect.isawaitable(result):
                    result = await result
                if result:
                    return
                await asyncio.sleep(interval)
    except TimeoutError:
        pytest.fail(f"Condition not met within {seconds}s: {message}")

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aio_pika
import asyncpg
import httpx
import pytest
from alembic import command
from alembic.config import Config
from faststream.rabbit import RabbitBroker
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from app.config import Settings
from app.consumer.app import create_consumer_app
from app.db.session import SessionFactory, create_session_factory
from app.messaging.broker import create_broker
from app.messaging.retry import retry_policy_from_settings
from app.messaging.topology import Topology, build_topology, declare_topology
from app.services.gateway import PaymentGateway
from tests.integration.helpers import ConsumerRunner, CountingGateway, WebhookRecorder, api_client_for
from tests.settings import TEST_DATABASE_URL, TEST_RABBITMQ_URL, make_settings

ROOT = Path(__file__).resolve().parents[2]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Хук сессионный: помечаем только тесты из этого каталога (`pytest -m "not integration"` — без Docker).
    here = Path(__file__).parent
    for item in items:
        if here in item.path.parents:
            item.add_marker(pytest.mark.integration)


def alembic_config(database_url: str) -> Config:
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    config.attributes["configure_logger"] = False
    return config


async def _ensure_database(database_url: str) -> None:
    url = make_url(database_url)
    conn = await asyncpg.connect(
        user=url.username, password=url.password, host=url.host, port=url.port or 5432, database="postgres"
    )
    try:
        if not await conn.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", url.database):
            await conn.execute(f'CREATE DATABASE "{url.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
async def database_url() -> str:
    if not TEST_DATABASE_URL or not TEST_RABBITMQ_URL:
        pytest.skip("TEST_DATABASE_URL / TEST_RABBITMQ_URL не заданы (запуск: make test)")
    await _ensure_database(TEST_DATABASE_URL)
    config = alembic_config(TEST_DATABASE_URL)
    # Полный цикл downgrade -> upgrade заодно проверяет, что миграции обратимы.
    # env.py вызывает asyncio.run(), поэтому alembic запускается в отдельном потоке.
    await asyncio.to_thread(command.downgrade, config, "base")
    await asyncio.to_thread(command.upgrade, config, "head")
    return TEST_DATABASE_URL


@pytest.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(database_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> SessionFactory:
    return create_session_factory(engine)


@pytest.fixture
async def settings(database_url: str) -> AsyncIterator[Settings]:
    """Уникальные имена exchange/очередей на тест — тесты не мешают друг другу и запущенному стеку."""
    suffix = uuid.uuid4().hex[:8]
    settings = make_settings(
        database_url=database_url,
        rabbitmq_exchange=f"test.{suffix}.payments",
        rabbitmq_queue=f"test.{suffix}.payments.new",
    )
    yield settings
    await _delete_topology(settings)


async def _delete_topology(settings: Settings) -> None:
    topology = build_topology(settings, retry_policy_from_settings(settings))
    connection = await aio_pika.connect(settings.rabbitmq_url)
    async with connection:
        channel = await connection.channel()
        for queue in (topology.queue, topology.dead_letter_queue, *topology.retry_queues):
            await channel.queue_delete(queue.name)
        for exchange in (topology.exchange, topology.retry_exchange, topology.dead_letter_exchange):
            await channel.exchange_delete(exchange.name)


@pytest.fixture
def topology(settings: Settings) -> Topology:
    return build_topology(settings, retry_policy_from_settings(settings))


@pytest.fixture
async def api_client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """API без outbox relay: тесты API проверяют только запись в БД и outbox."""
    async with api_client_for(settings.model_copy(update={"outbox_relay_enabled": False})) as client:
        yield client


@pytest.fixture
async def publisher_broker(settings: Settings, topology: Topology) -> AsyncIterator[RabbitBroker]:
    broker = create_broker(settings)
    await broker.start()
    await declare_topology(broker, topology)
    yield broker
    await broker.stop()


@pytest.fixture
def run_consumer(settings: Settings) -> ConsumerRunner:
    @asynccontextmanager
    async def runner(
        webhook: WebhookRecorder, gateway: PaymentGateway | None = None, **overrides: Any
    ) -> AsyncIterator[None]:
        consumer_settings = settings.model_copy(update=overrides) if overrides else settings
        http_client = httpx.AsyncClient(transport=httpx.MockTransport(webhook))
        app = create_consumer_app(consumer_settings, gateway=gateway or CountingGateway(), http_client=http_client)
        await app.start()
        try:
            yield
        finally:
            await app.stop()

    return runner

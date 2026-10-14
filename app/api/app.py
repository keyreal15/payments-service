import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI

from app.api.dependencies import AppContainer, verify_api_key
from app.api.errors import register_exception_handlers
from app.api.health import router as health_router
from app.api.middleware import ApiGuardMiddleware
from app.api.v1.payments import router as payments_router
from app.config import Settings, get_settings
from app.db.session import create_db_engine, create_session_factory
from app.messaging.broker import create_broker
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.publisher import RabbitEventPublisher
from app.messaging.retry import retry_policy_from_settings
from app.messaging.topology import build_topology

logger = logging.getLogger(__name__)

RELAY_STOP_TIMEOUT = 10.0


def _report_relay_exit(task: asyncio.Task[None]) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.critical("Outbox relay crashed, events are not being published", exc_info=task.exception())


async def _stop_relay(relay: OutboxRelay, task: asyncio.Task[None]) -> None:
    relay.stop()
    try:
        await asyncio.wait_for(task, timeout=RELAY_STOP_TIMEOUT)  # по таймауту задача отменяется
    except TimeoutError:
        logger.warning("Outbox relay did not stop within %.0fs and was cancelled", RELAY_STOP_TIMEOUT)
    except Exception:
        logger.exception("Outbox relay finished with an error")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    if settings.api_key is None:
        raise RuntimeError("API_KEY must be set to run the API")
    api_key = settings.api_key.get_secret_value()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_db_engine(settings, application_name="payments-api")
        broker = create_broker(settings)
        topology = build_topology(settings, retry_policy_from_settings(settings))
        session_factory = create_session_factory(engine)

        # Брокер не ждём на старте: подключение (с ретраями) выполняет relay в фоне.
        # API принимает платежи и без RabbitMQ — события копятся в outbox.
        relay: OutboxRelay | None = None
        relay_task: asyncio.Task[None] | None = None
        if settings.outbox_relay_enabled:
            relay = OutboxRelay(
                session_factory,
                RabbitEventPublisher(broker, topology, timeout=settings.rabbitmq_publish_timeout),
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval,
                retry_base_delay=settings.outbox_retry_base_delay,
                retry_max_delay=settings.outbox_retry_max_delay,
            )
            relay_task = asyncio.create_task(relay.run(), name="outbox-relay")
            relay_task.add_done_callback(_report_relay_exit)

        app.state.container = AppContainer(
            api_key=api_key,
            session_factory=session_factory,
            broker=broker,
            topology=topology,
            relay=relay,
            relay_task=relay_task,
        )
        try:
            yield
        finally:
            try:
                if relay is not None and relay_task is not None:
                    await _stop_relay(relay, relay_task)
                await broker.stop()
            finally:
                await engine.dispose()
            logger.info("API shutdown complete")

    app = FastAPI(
        title="Payments Service",
        version="1.0.0",
        description="Асинхронный процессинг платежей: Outbox + RabbitMQ + webhook-уведомления.",
        lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
    )
    app.add_middleware(ApiGuardMiddleware, api_key=api_key, max_body_size=settings.api_max_body_size)
    register_exception_handlers(app)

    api_v1 = APIRouter(prefix="/api/v1", dependencies=[Depends(verify_api_key)])
    api_v1.include_router(payments_router)
    app.include_router(api_v1)
    app.include_router(health_router)
    return app

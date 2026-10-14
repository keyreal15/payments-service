import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import APIRouter, Depends, FastAPI

from app.api.background import BackgroundWorker, start_worker, stop_worker
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
from app.services.reconciliation import StalePaymentsReconciler

logger = logging.getLogger(__name__)


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

        # Брокер не ждём на старте: к нему подключается relay (с ретраями), а API принимает платежи
        # и без RabbitMQ — события копятся в outbox.
        relay: OutboxRelay | None = None
        workers: dict[str, BackgroundWorker] = {}
        if settings.outbox_relay_enabled:
            relay = OutboxRelay(
                session_factory,
                RabbitEventPublisher(broker, topology, timeout=settings.rabbitmq_publish_timeout),
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval,
                retry_base_delay=settings.outbox_retry_base_delay,
                retry_max_delay=settings.outbox_retry_max_delay,
            )
            workers["outbox_relay"] = relay
            workers["reconciler"] = StalePaymentsReconciler(
                session_factory,
                routing_key=topology.routing_key,
                stale_after=settings.reconcile_stale_after,
                interval=settings.reconcile_interval,
                on_enqueued=relay.notify,
            )
        tasks = {name: start_worker(name, worker) for name, worker in workers.items()}

        app.state.container = AppContainer(
            api_key=api_key,
            session_factory=session_factory,
            broker=broker,
            topology=topology,
            relay=relay,
            background_tasks=tasks,
        )
        try:
            yield
        finally:
            try:
                for name, worker in workers.items():
                    await stop_worker(worker, tasks[name])
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

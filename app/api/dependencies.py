import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated

from fastapi import Depends, Header, Request, status
from fastapi.security import APIKeyHeader
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.errors import ApiError
from app.api.middleware import API_KEY_HEADER, api_key_matches
from app.db.session import SessionFactory
from app.messaging.outbox_relay import OutboxRelay
from app.messaging.topology import Topology
from app.services.payments import PaymentService


@dataclass(slots=True)
class AppContainer:
    api_key: str
    session_factory: SessionFactory
    broker: RabbitBroker
    topology: Topology
    relay: OutboxRelay | None
    relay_task: asyncio.Task[None] | None


def get_container(request: Request) -> AppContainer:
    container: AppContainer = request.app.state.container
    return container


ContainerDep = Annotated[AppContainer, Depends(get_container)]

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False, description="Статический API-ключ")


async def verify_api_key(
    container: ContainerDep,
    api_key: Annotated[str | None, Depends(_api_key_header)],
) -> None:
    """Ключ уже проверен ApiGuardMiddleware до чтения тела; зависимость описывает security-схему
    в OpenAPI (кнопка Authorize в Swagger) и защищает роут, если его подключат без middleware."""
    if not api_key_matches(api_key, container.api_key):
        raise ApiError(
            status.HTTP_401_UNAUTHORIZED,
            "unauthorized",
            "Invalid or missing API key",
            headers={"WWW-Authenticate": "APIKey"},
        )


async def get_session(container: ContainerDep) -> AsyncIterator[AsyncSession]:
    async with container.session_factory() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]


def get_payment_service(container: ContainerDep, session: SessionDep) -> PaymentService:
    return PaymentService(
        session,
        routing_key=container.topology.routing_key,
        on_event_committed=container.relay.notify if container.relay else None,
    )


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        pattern=r"^[\x21-\x7E]+$",
        description="Уникальный ключ запроса (печатные ASCII-символы без пробелов, до 255). "
        "Повтор с тем же ключом и телом вернёт уже созданный платёж.",
    ),
]

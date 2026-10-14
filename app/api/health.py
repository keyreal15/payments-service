import asyncio
import logging
from typing import Literal

from fastapi import APIRouter, Response, status
from pydantic import BaseModel
from sqlalchemy import func, select, text

from app.api.dependencies import ContainerDep
from app.db.models import OutboxMessage

logger = logging.getLogger(__name__)

# Служебные эндпоинты для оркестратора (Docker healthcheck / k8s probes) — без API-ключа,
# бизнес-данных не раскрывают.
router = APIRouter(prefix="/health", tags=["health"])

CHECK_TIMEOUT = 2.0

State = Literal["ok", "unavailable"]


class Liveness(BaseModel):
    status: Literal["ok"] = "ok"


class Readiness(BaseModel):
    status: State
    database: State
    rabbitmq: Literal["ok", "unavailable", "disabled"]
    outbox_backlog: int | None
    background_tasks: dict[str, Literal["running", "stopped"]]


@router.get("/live", summary="Liveness probe")
async def live() -> Liveness:
    return Liveness()


@router.get(
    "/ready",
    summary="Readiness probe",
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": Readiness,
            "description": "PostgreSQL недоступна или упала фоновая задача (outbox relay, сверка)",
        }
    },
)
async def ready(container: ContainerDep, response: Response) -> Readiness:
    """Готовность: PostgreSQL доступна и фоновые задачи живы. Без RabbitMQ API продолжает принимать
    платежи (события копятся в outbox), поэтому брокер и размер outbox — информационные поля."""
    backlog: int | None = None
    try:
        async with asyncio.timeout(CHECK_TIMEOUT), container.session_factory() as session:
            await session.execute(text("SELECT 1"))
            backlog = await session.scalar(
                select(func.count()).select_from(OutboxMessage).where(OutboxMessage.published_at.is_(None))
            )
        database: State = "ok"
    except Exception:
        logger.warning("Readiness check: database is unavailable", exc_info=True)
        database = "unavailable"

    rabbitmq: Literal["ok", "unavailable", "disabled"] = "disabled"  # relay выключен — брокер API не нужен
    if container.relay is not None:
        rabbitmq = "ok" if await container.broker.ping(timeout=1.0) else "unavailable"
    tasks: dict[str, Literal["running", "stopped"]] = {
        name: "stopped" if task.done() else "running" for name, task in container.background_tasks.items()
    }

    overall: State = "ok" if database == "ok" and "stopped" not in tasks.values() else "unavailable"
    if overall != "ok":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return Readiness(
        status=overall, database=database, rabbitmq=rabbitmq, outbox_backlog=backlog, background_tasks=tasks
    )

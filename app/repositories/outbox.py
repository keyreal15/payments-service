import uuid
from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxMessage


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, *, event_type: str, aggregate_id: uuid.UUID, routing_key: str, payload: dict[str, Any]) -> None:
        self._session.add(
            OutboxMessage(
                event_type=event_type,
                aggregate_id=aggregate_id,
                routing_key=routing_key,
                payload=payload,
            )
        )

    async def lock_pending_batch(self, limit: int) -> Sequence[OutboxMessage]:
        """Берёт пачку готовых к отправке событий под блокировку.

        SKIP LOCKED позволяет запускать несколько relay (несколько реплик API)
        без двойной публикации одного и того же события.
        """
        stmt = (
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None), OutboxMessage.next_attempt_at <= func.now())
            .order_by(OutboxMessage.next_attempt_at, OutboxMessage.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
        stmt = (
            update(OutboxMessage)
            .where(OutboxMessage.id.in_(ids))
            .values(published_at=func.statement_timestamp())
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)

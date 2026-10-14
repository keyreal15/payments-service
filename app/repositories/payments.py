import uuid
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.enums import Currency, PaymentStatus


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_if_absent(
        self,
        *,
        amount: Decimal,
        currency: Currency,
        description: str | None,
        metadata: dict[str, Any],
        webhook_url: str,
        idempotency_key: str,
        request_hash: str,
    ) -> Payment | None:
        """INSERT ... ON CONFLICT (idempotency_key) DO NOTHING.

        Возвращает созданный платёж или None, если платёж с таким ключом уже есть.
        Конкурентная вставка с тем же ключом дождётся коммита первой транзакции —
        гонки между SELECT и INSERT нет.
        """
        stmt = (
            insert(Payment)
            .values(
                amount=amount,
                currency=currency,
                description=description,
                meta=metadata,
                webhook_url=webhook_url,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def lock_pending(self, payment_id: uuid.UUID) -> Payment | None:
        """Блокирует pending-платёж для обработки (SELECT ... FOR NO KEY UPDATE SKIP LOCKED).

        None — платёж не pending или прямо сейчас заблокирован другим обработчиком.
        NO KEY: ключевые колонки не меняются, будущие FK на payments не будут ждать эту блокировку.
        """
        stmt = (
            select(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .with_for_update(skip_locked=True, key_share=True)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self._session.scalars(stmt)).one_or_none()

    async def finalize(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None:
        """Compare-and-set pending -> final. None, если платёж уже был финализирован кем-то ещё."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            # statement_timestamp(), а не now(): now() — время начала транзакции, а она открыта
            # ещё до вызова шлюза (см. PaymentProcessor), processed_at был бы раньше реального.
            .values(status=status, processed_at=func.statement_timestamp())
            .returning(Payment)
            # populate_existing: если платёж уже загружен в сессию, обновить его атрибуты из RETURNING,
            # иначе identity map вернёт объект со старым статусом.
            .execution_options(synchronize_session=False, populate_existing=True)
        )
        return (await self._session.scalars(stmt)).one_or_none()

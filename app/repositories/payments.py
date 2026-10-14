import uuid
from collections.abc import Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutboxMessage, Payment
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

    async def acquire_lease(self, payment_id: uuid.UUID, lease_id: uuid.UUID, duration: timedelta) -> Payment | None:
        """Берёт pending-платёж в обработку: аренда свободна, истекла или уже наша (повтор того же сообщения).

        None — платёж не pending или аренда занята другим обработчиком.
        """
        now = func.statement_timestamp()
        stmt = (
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.status == PaymentStatus.PENDING,
                or_(
                    Payment.lease_expires_at.is_(None),
                    Payment.lease_expires_at < now,
                    Payment.lease_id == lease_id,
                ),
            )
            .values(lease_id=lease_id, lease_expires_at=now + duration)
            .returning(Payment)
            .execution_options(synchronize_session=False, populate_existing=True)
        )
        return (await self._session.scalars(stmt)).one_or_none()

    async def release_lease(self, payment_id: uuid.UUID, lease_id: uuid.UUID) -> None:
        """Отпускает свою аренду (после сбоя шлюза), чтобы повтор не ждал её истечения."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.lease_id == lease_id, Payment.status == PaymentStatus.PENDING)
            .values(lease_id=None, lease_expires_at=None)
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)

    async def lock_stale_pending(self, stale_after: timedelta, limit: int) -> Sequence[uuid.UUID]:
        """Pending-платежи старше stale_after без активной аренды, по которым за это время не создавалось событий.

        SKIP LOCKED: несколько реплик API не возьмут одни и те же платежи.
        """
        now = func.now()
        recent_event = (
            select(OutboxMessage.id)
            .where(OutboxMessage.aggregate_id == Payment.id, OutboxMessage.created_at > now - stale_after)
            .exists()
        )
        stmt = (
            select(Payment.id)
            .where(
                Payment.status == PaymentStatus.PENDING,
                Payment.created_at < now - stale_after,
                or_(Payment.lease_expires_at.is_(None), Payment.lease_expires_at < now),
                ~recent_event,
            )
            .order_by(Payment.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True, of=Payment)
        )
        return (await self._session.scalars(stmt)).all()

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self._session.scalars(stmt)).one_or_none()

    async def finalize(self, payment_id: uuid.UUID, status: PaymentStatus) -> Payment | None:
        """Compare-and-set pending -> final. None, если платёж уже финализирован кем-то ещё."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.statement_timestamp(), lease_id=None, lease_expires_at=None)
            .returning(Payment)
            # populate_existing: объект из identity map получит значения из RETURNING, а не останется старым.
            .execution_options(synchronize_session=False, populate_existing=True)
        )
        return (await self._session.scalars(stmt)).one_or_none()

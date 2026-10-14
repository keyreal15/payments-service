import asyncio
import contextlib
import logging
from collections.abc import Callable
from datetime import timedelta

from app.db.session import SessionFactory
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)

PAYMENT_RECONCILIATION_EVENT = "payment.reconciliation"


class StalePaymentsReconciler:
    """Возвращает в обработку платежи, зависшие в pending.

    Так бывает, если сообщение ушло в DLQ после серии сбоев шлюза или потерялось. Через outbox
    публикуется повторное событие; это безопасно: шлюз идемпотентен по payment_id, поэтому
    для уже проведённого у него платежа вернётся прежний результат, а не новое списание.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        routing_key: str,
        stale_after: float,
        interval: float,
        batch_size: int = 100,
        on_enqueued: Callable[[], None] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._routing_key = routing_key
        self._stale_after = timedelta(seconds=stale_after)
        self._interval = interval
        self._batch_size = batch_size
        self._on_enqueued = on_enqueued
        self._stopped = asyncio.Event()

    def stop(self) -> None:
        self._stopped.set()

    async def reconcile_once(self) -> int:
        async with self._session_factory() as session, session.begin():
            payment_ids = await PaymentRepository(session).lock_stale_pending(self._stale_after, self._batch_size)
            outbox = OutboxRepository(session)
            for payment_id in payment_ids:
                outbox.add(
                    event_type=PAYMENT_RECONCILIATION_EVENT,
                    aggregate_id=payment_id,
                    routing_key=self._routing_key,
                    payload=PaymentEvent(payment_id=payment_id).model_dump(mode="json"),
                )
        if payment_ids:
            logger.warning("Re-enqueued %d payment(s) stuck in pending: %s", len(payment_ids), payment_ids)
            if self._on_enqueued is not None:
                self._on_enqueued()
        return len(payment_ids)

    async def run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.reconcile_once()
            except Exception:
                logger.exception("Reconciliation of stale payments failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._interval)

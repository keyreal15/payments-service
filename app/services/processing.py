import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TypeVar

from app.db.errors import is_database_unavailable
from app.db.models import Payment
from app.db.session import SessionFactory
from app.enums import PaymentStatus
from app.repositories.payments import PaymentRepository
from app.services.errors import DatabaseUnavailableError, PaymentBusyError, PaymentNotFoundError
from app.services.gateway import GatewayError, PaymentGateway
from app.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)

T = TypeVar("T")


class PaymentProcessor:
    """Обработка события по платежу: шлюз -> статус в БД -> webhook. Безопасна при повторной и параллельной доставке.

    Платёж проводится в три коротких шага, транзакция не держится во время вызова шлюза:
      1. аренда: pending-платёж помечается lease_id/lease_expires_at. Пока аренда жива, параллельный дубль
         получает PaymentBusyError и откладывается; если обработчик умрёт, аренда истечёт и платёж подхватит повтор;
      2. вызов шлюза с idempotency key = payment_id (повторный вызов вернёт тот же результат, а не спишет снова);
      3. фиксация результата compare-and-set по статусу pending.
    Для финализированного платежа шаги 1–3 пропускаются — повторяется только webhook.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        *,
        gateway_timeout: float,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._webhooks = webhooks
        self._gateway_timeout = gateway_timeout
        # Аренда с запасом переживает вызов шлюза, который ограничен gateway_timeout.
        self._lease_duration = timedelta(seconds=gateway_timeout * 2)

    async def process(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._settle(payment_id)
        await self._webhooks.send(payment)
        logger.info("Webhook for payment %s (%s) delivered", payment_id, payment.status.value)
        return payment

    async def _settle(self, payment_id: uuid.UUID) -> Payment:
        lease_id = uuid.uuid4()
        payment = await self._in_transaction(lambda repo: self._acquire(repo, payment_id, lease_id))
        if payment.status.is_final:
            logger.info("Payment %s already %s, skipping gateway call", payment_id, payment.status.value)
            return payment

        try:
            status = await self._charge(payment)
        except BaseException:
            await self._release_quietly(payment_id, lease_id)
            raise

        finalized = await self._in_transaction(lambda repo: self._finalize(repo, payment_id, status))
        logger.info("Payment %s processed: %s", payment_id, finalized.status.value)
        return finalized

    async def _charge(self, payment: Payment) -> PaymentStatus:
        try:
            async with asyncio.timeout(self._gateway_timeout):
                return await self._gateway.charge(
                    idempotency_key=str(payment.id), amount=payment.amount, currency=payment.currency
                )
        except TimeoutError as exc:
            raise GatewayError(f"Gateway did not respond within {self._gateway_timeout}s") from exc

    async def _acquire(self, repo: PaymentRepository, payment_id: uuid.UUID, lease_id: uuid.UUID) -> Payment:
        payment = await repo.acquire_lease(payment_id, lease_id, self._lease_duration)
        if payment is not None:
            return payment
        current = await repo.get(payment_id)
        if current is None:
            raise PaymentNotFoundError(payment_id)
        if not current.status.is_final:
            raise PaymentBusyError(payment_id)
        return current

    @staticmethod
    async def _finalize(repo: PaymentRepository, payment_id: uuid.UUID, status: PaymentStatus) -> Payment:
        payment = await repo.finalize(payment_id, status)
        if payment is None:
            # Аренда успела истечь, и платёж финализировал другой обработчик — тем же результатом шлюза.
            payment = await repo.get(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
        return payment

    async def _release_quietly(self, payment_id: uuid.UUID, lease_id: uuid.UUID) -> None:
        try:
            await self._in_transaction(lambda repo: repo.release_lease(payment_id, lease_id))
        except Exception:
            logger.warning("Failed to release lease of payment %s, it will expire", payment_id, exc_info=True)

    async def _in_transaction(self, operation: Callable[[PaymentRepository], Awaitable[T]]) -> T:
        try:
            async with self._session_factory() as session, session.begin():
                return await operation(PaymentRepository(session))
        except Exception as exc:
            if is_database_unavailable(exc):
                raise DatabaseUnavailableError(f"Database is unavailable: {exc!r}") from exc
            raise

import asyncio
import logging
import uuid

from app.db.errors import is_database_unavailable
from app.db.models import Payment
from app.db.session import SessionFactory
from app.repositories.payments import PaymentRepository
from app.services.errors import (
    DatabaseUnavailableError,
    PaymentBusyError,
    PaymentNotFoundError,
    PermanentProcessingError,
    TransientProcessingError,
)
from app.services.gateway import GatewayError, PaymentGateway
from app.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Обработка события payments.new. Идемпотентна: безопасна при повторной и параллельной доставке.

    1. Pending-платёж блокируется (SELECT ... FOR NO KEY UPDATE SKIP LOCKED), проводится через шлюз
       с idempotency key = payment.id, результат фиксируется в той же транзакции:
         * параллельный дубль видит блокировку -> PaymentBusyError -> сообщение откладывается
           (не теряется: если владелец блокировки умрёт, дубль обработает платёж позже);
         * дубль после фиксации видит финальный статус и шлюз не вызывает.
       Транзакция держится на время вызова шлюза — осознанный компромисс ради отсутствия
       параллельных списаний. Её длительность ограничена gateway_timeout, а «зависшие» сессии
       PostgreSQL обрывает по idle_in_transaction_session_timeout (см. create_consumer_app).
    2. Webhook отправляется после коммита, вне транзакции. Ошибка -> исключение -> retry;
       при повторе шаг 1 пропускается и статус не меняется.

    Недоступность PostgreSQL на шаге 1 превращается в DatabaseUnavailableError — сообщение
    откладывается без расхода попытки (короткий сбой БД не должен отправлять всё в DLQ).
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

    async def process(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._charge_if_pending(payment_id)
        await self._webhooks.send(payment)
        logger.info("Webhook for payment %s (%s) delivered", payment_id, payment.status.value)
        return payment

    async def _charge_if_pending(self, payment_id: uuid.UUID) -> Payment:
        try:
            return await self._charge_if_pending_in_transaction(payment_id)
        except (GatewayError, PermanentProcessingError, TransientProcessingError):
            raise
        except Exception as exc:
            if is_database_unavailable(exc):
                raise DatabaseUnavailableError(f"Database is unavailable: {exc!r}") from exc
            raise

    async def _charge_if_pending_in_transaction(self, payment_id: uuid.UUID) -> Payment:
        async with self._session_factory() as session, session.begin():
            repo = PaymentRepository(session)
            payment = await repo.lock_pending(payment_id)

            if payment is None:
                current = await repo.get(payment_id)
                if current is None:
                    raise PaymentNotFoundError(payment_id)
                if not current.status.is_final:
                    raise PaymentBusyError(payment_id)
                logger.info("Payment %s already %s, skipping gateway call", payment_id, current.status.value)
                return current

            try:
                async with asyncio.timeout(self._gateway_timeout):
                    status = await self._gateway.charge(
                        idempotency_key=str(payment.id), amount=payment.amount, currency=payment.currency
                    )
            except TimeoutError as exc:
                raise GatewayError(f"Gateway did not respond within {self._gateway_timeout}s") from exc

            finalized = await repo.finalize(payment_id, status)
            if finalized is None:  # строка заблокирована нами — измениться не могла
                raise RuntimeError(f"Payment {payment_id} changed while locked")
            logger.info("Payment %s processed: %s", payment_id, finalized.status.value)
            return finalized

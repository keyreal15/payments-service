import uuid
from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentCreate, PaymentEvent
from app.services.errors import IdempotencyKeyReusedError, PaymentNotFoundError

PAYMENT_CREATED_EVENT = "payment.created"


@dataclass(frozen=True, slots=True)
class CreatePaymentResult:
    payment: Payment
    created: bool


class PaymentService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        routing_key: str,
        on_event_committed: Callable[[], None] | None = None,
    ) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)
        self._routing_key = routing_key
        self._on_event_committed = on_event_committed

    async def create(self, data: PaymentCreate, idempotency_key: str) -> CreatePaymentResult:
        request_hash = data.fingerprint()

        # Платёж и событие в outbox пишутся в одной транзакции: либо оба, либо ничего.
        async with self._session.begin():
            payment = await self._payments.create_if_absent(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata=data.metadata,
                webhook_url=str(data.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            if payment is not None:
                self._outbox.add(
                    event_type=PAYMENT_CREATED_EVENT,
                    aggregate_id=payment.id,
                    routing_key=self._routing_key,
                    payload=PaymentEvent(payment_id=payment.id).model_dump(mode="json"),
                )
                created = True
            else:
                payment = await self._payments.get_by_idempotency_key(idempotency_key)
                if payment is None:  # ON CONFLICT гарантирует, что строка есть
                    raise RuntimeError("Idempotency conflict without existing payment")
                if payment.request_hash != request_hash:
                    raise IdempotencyKeyReusedError(idempotency_key)
                created = False

        if created and self._on_event_committed is not None:
            self._on_event_committed()
        return CreatePaymentResult(payment=payment, created=created)

    async def get(self, payment_id: uuid.UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

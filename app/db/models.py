import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Enum, Index, Integer, Numeric, String, Text, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.enums import Currency, PaymentStatus


def _enum_values(enum_cls: type[StrEnum]) -> list[str]:
    return [member.value for member in enum_cls]


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(
        Enum(
            Currency,
            name="currency",
            native_enum=False,
            create_constraint=True,
            length=3,
            values_callable=_enum_values,
        )
    )
    description: Mapped[str | None] = mapped_column(String(512))
    # Атрибут `metadata` зарезервирован в Declarative API, поэтому в ORM поле называется `meta`,
    # а колонка в БД — `metadata`, как поле в API.
    meta: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, default=dict, server_default=text("'{}'::jsonb"))
    status: Mapped[PaymentStatus] = mapped_column(
        Enum(
            PaymentStatus,
            name="payment_status",
            native_enum=False,
            create_constraint=True,
            length=16,
            values_callable=_enum_values,
        ),
        default=PaymentStatus.PENDING,
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    # SHA-256 нормализованного тела запроса: повтор с тем же ключом, но другим телом — ошибка клиента.
    request_hash: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Аренда на обработку: кто и до какого момента проводит платёж через шлюз (см. PaymentProcessor).
    lease_id: Mapped[uuid.UUID | None]
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("amount > 0", name="amount_positive"),
        CheckConstraint("(status = 'pending') = (processed_at IS NULL)", name="processed_at_matches_status"),
        CheckConstraint(
            "status = 'pending' OR (lease_id IS NULL AND lease_expires_at IS NULL)", name="lease_only_while_pending"
        ),
    )


class OutboxMessage(Base):
    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(64))
    aggregate_id: Mapped[uuid.UUID]
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_error: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        # Частичный индекс: relay сканирует только неопубликованные события.
        Index(
            "ix_outbox_unpublished",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.db.base import Base

INSERT = text(
    "INSERT INTO payments (id, amount, currency, status, idempotency_key, request_hash, webhook_url, processed_at) "
    "VALUES (gen_random_uuid(), :amount, :currency, :status, :key, 'h', 'http://x', :processed_at)"
)
PROCESSED = datetime(2026, 1, 1, tzinfo=UTC)
VALID_PENDING: dict[str, Any] = {"amount": 1, "currency": "RUB", "status": "pending", "processed_at": None}


async def test_models_match_migrations(engine: AsyncEngine) -> None:
    def diff(connection: Connection) -> list[object]:
        context = MigrationContext.configure(connection, opts={"compare_type": True, "compare_server_default": True})
        return list(compare_metadata(context, Base.metadata))

    async with engine.connect() as conn:
        assert await conn.run_sync(diff) == []


async def test_check_constraints_exist(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        names = set(
            (await conn.execute(text("SELECT conname FROM pg_constraint WHERE conrelid = 'payments'::regclass")))
            .scalars()
            .all()
        )
    assert {
        "ck_payments_amount_positive",
        "ck_payments_currency",
        "ck_payments_payment_status",
        "ck_payments_processed_at_matches_status",
        "uq_payments_idempotency_key",
    } <= names


@pytest.mark.parametrize(
    ("override", "constraint"),
    [
        ({"amount": 0}, "ck_payments_amount_positive"),
        ({"currency": "GBP"}, "ck_payments_currency"),
        # processed_at задан, чтобы не сработало ограничение на processed_at — проверяем именно статус.
        ({"status": "unknown", "processed_at": PROCESSED}, "ck_payments_payment_status"),
        ({"status": "succeeded"}, "ck_payments_processed_at_matches_status"),
        ({"processed_at": PROCESSED}, "ck_payments_processed_at_matches_status"),
    ],
)
async def test_database_constraints(engine: AsyncEngine, override: dict[str, Any], constraint: str) -> None:
    with pytest.raises(IntegrityError, match=constraint):
        async with engine.begin() as conn:
            await conn.execute(INSERT, {**VALID_PENDING, **override, "key": uuid.uuid4().hex})


async def test_valid_rows_pass_constraints(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(INSERT, {**VALID_PENDING, "key": uuid.uuid4().hex})
        await conn.execute(
            INSERT, {**VALID_PENDING, "status": "succeeded", "processed_at": PROCESSED, "key": uuid.uuid4().hex}
        )

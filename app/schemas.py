import hashlib
import json
import math
import re
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    AliasChoices,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    HttpUrl,
    StringConstraints,
    UrlConstraints,
    WithJsonSchema,
    field_validator,
)

from app.enums import Currency, PaymentStatus

MAX_METADATA_BYTES = 16 * 1024
MAX_METADATA_DEPTH = 10
MAX_URL_LENGTH = 2048
AMOUNT_PATTERN = re.compile(r"^[0-9]{1,16}(\.[0-9]{1,2})?$")
# Любое десятичное число до 15 значащих цифр однозначно восстанавливается из float (IEEE 754 double).
MAX_FLOAT_SIGNIFICANT_DIGITS = 15


def _parse_amount(value: object) -> object:
    """Строка — строго `1500` / `1500.50`. JSON-число приходит из парсера уже как float и принимается,
    только если восстанавливается однозначно; иначе клиент должен передать строку."""
    if isinstance(value, bool):
        raise ValueError("amount must be a decimal string or a number")
    if isinstance(value, str):
        if not AMOUNT_PATTERN.fullmatch(value):
            raise ValueError("amount must be a decimal string like '1500' or '1500.00' (up to 2 decimal places)")
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("amount must be a finite number")
        exact = Decimal(repr(value))
        if len(exact.normalize().as_tuple().digits) > MAX_FLOAT_SIGNIFICANT_DIGITS:
            raise ValueError("amount has too many significant digits for a JSON number, pass it as a string")
        return exact
    return value


def _reject_unsafe_chars(value: str) -> str:
    # PostgreSQL не хранит NUL в text/jsonb, а одиночные суррогаты не кодируются в UTF-8 —
    # без этой проверки такие значения превратились бы в 500 вместо 422.
    if "\x00" in value:
        raise ValueError("must not contain NUL characters")
    if any("\ud800" <= char <= "\udfff" for char in value):
        raise ValueError("must not contain unpaired surrogate characters")
    return value


def _check_url_length(url: HttpUrl) -> HttpUrl:
    # UrlConstraints проверяет исходную строку, а в БД пишется нормализованная (percent-encoding, punycode).
    if len(str(url)) > MAX_URL_LENGTH:
        raise ValueError(f"webhook_url must not exceed {MAX_URL_LENGTH} characters after normalization")
    return url


def _validate_json_tree(value: object, max_depth: int) -> None:
    """Обход без рекурсии: глубина, NaN/Infinity, NUL и суррогаты в ключах и значениях."""
    stack: list[tuple[object, int]] = [(value, 1)]
    while stack:
        item, depth = stack.pop()
        if isinstance(item, str):
            _reject_unsafe_chars(item)
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("must not contain NaN or Infinity")
        elif isinstance(item, dict | list):
            if depth > max_depth:
                raise ValueError(f"nesting must not exceed {max_depth} levels")
            if isinstance(item, dict):
                for key, child in item.items():
                    _reject_unsafe_chars(key)
                    stack.append((child, depth + 1))
            else:
                stack.extend((child, depth + 1) for child in item)


Amount = Annotated[
    Decimal,
    BeforeValidator(_parse_amount),
    Field(gt=0, max_digits=18, decimal_places=2),
    WithJsonSchema(
        {
            "anyOf": [
                {"type": "string", "pattern": AMOUNT_PATTERN.pattern},
                {"type": "number", "exclusiveMinimum": 0},
            ],
            "description": "Сумма > 0, не более 2 знаков после запятой. Рекомендуется строкой: числа "
            "с более чем 15 значащими цифрами в JSON теряют точность и отклоняются.",
            "examples": ["1500.00"],
        }
    ),
]
Description = Annotated[
    str, StringConstraints(strip_whitespace=True, max_length=512), AfterValidator(_reject_unsafe_chars)
]
WebhookUrl = Annotated[HttpUrl, UrlConstraints(max_length=MAX_URL_LENGTH), AfterValidator(_check_url_length)]


class PaymentCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "amount": "1500.00",
                    "currency": "RUB",
                    "description": "Оплата заказа #42",
                    "metadata": {"order_id": 42},
                    "webhook_url": "http://webhook-receiver:9000/webhooks/ok",
                }
            ]
        },
    )

    amount: Amount
    currency: Currency
    description: Description | None = None
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description=f"Произвольный JSON-объект: до {MAX_METADATA_BYTES} байт, вложенность до {MAX_METADATA_DEPTH}",
    )
    webhook_url: WebhookUrl

    @field_validator("description")
    @classmethod
    def _blank_description_to_none(cls, value: str | None) -> str | None:
        return value or None

    @field_validator("metadata")
    @classmethod
    def _validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        _validate_json_tree(value, MAX_METADATA_DEPTH)
        if len(json.dumps(value, ensure_ascii=False).encode()) > MAX_METADATA_BYTES:
            raise ValueError(f"must not exceed {MAX_METADATA_BYTES} bytes when serialized")
        return value

    def fingerprint(self) -> str:
        """SHA-256 канонического тела: одинаковый для семантически одинаковых запросов
        (`"100"`, `100` и `"100.00"`; любой порядок ключей metadata)."""
        canonical = {
            "amount": str(self.amount.quantize(Decimal("0.01"))),
            "currency": self.currency.value,
            "description": self.description,
            "metadata": self.metadata,
            "webhook_url": str(self.webhook_url),
        }
        raw = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(raw.encode()).hexdigest()


class PaymentAccepted(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(validation_alias=AliasChoices("payment_id", "id"))
    amount: Annotated[Decimal, WithJsonSchema({"type": "string", "examples": ["1500.00"]})]
    currency: Currency
    description: str | None
    # В ORM-модели атрибут называется `meta`, а `Payment.metadata` — это MetaData SQLAlchemy
    # (см. app/db/models.py). AliasChoices берёт первый существующий атрибут, поэтому `meta` идёт первым.
    metadata: dict[str, Any] = Field(validation_alias=AliasChoices("meta", "metadata"))
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class PaymentEvent(BaseModel):
    """Сообщение в очереди payments.new (payment.created / payment.reconciliation).

    Источник истины — БД, поэтому в событии только идентификатор."""

    payment_id: uuid.UUID


class WebhookPayload(BaseModel):
    event: Literal["payment.succeeded", "payment.failed"]
    payment: PaymentRead

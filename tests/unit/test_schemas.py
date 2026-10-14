from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from app.enums import Currency
from app.schemas import MAX_METADATA_BYTES, MAX_METADATA_DEPTH, PaymentCreate


def payload(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Order #1",
        "metadata": {"order_id": 1},
        "webhook_url": "https://example.com/hook",
    }
    data.update(overrides)
    return data


def nested(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {"leaf": 1}
    for _ in range(depth - 1):
        value = {"a": value}
    return value


def test_valid_payload() -> None:
    model = PaymentCreate.model_validate(payload())
    assert model.amount == Decimal("100.50")
    assert model.currency is Currency.RUB
    assert str(model.webhook_url) == "https://example.com/hook"


@pytest.mark.parametrize(
    ("amount", "expected"),
    [
        ("100", Decimal("100")),
        ("0.01", Decimal("0.01")),
        ("9999999999999999.99", Decimal("9999999999999999.99")),
        (100, Decimal("100")),
        (100.5, Decimal("100.5")),
        (1234567890123.45, Decimal("1234567890123.45")),  # 15 значащих цифр — float восстанавливается точно
        (1e14, Decimal("100000000000000")),  # нули в конце не считаются значащими
        (1500000000000000.0, Decimal("1500000000000000")),
    ],
)
def test_amount_accepts_exact_values(amount: Any, expected: Decimal) -> None:
    assert PaymentCreate.model_validate(payload(amount=amount)).amount == expected


@pytest.mark.parametrize(
    "amount",
    [
        "0",
        "-1",
        "0.001",
        "10.555",
        "abc",
        "99999999999999999.99",  # 17 цифр до запятой — не влезает в NUMERIC(18,2)
        "1_000",
        "+10",
        " 10 ",
        "1E+2",
        "１０",  # полноширинные цифры
        "NaN",
        float("nan"),
        float("inf"),
        True,
        99999999999999.99,  # как float это уже 99999999999999.98 — точность потеряна, отклоняем
        0,
    ],
)
def test_amount_rejects_invalid_or_lossy_values(amount: Any) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload(amount=amount))


@pytest.mark.parametrize("currency", ["GBP", "rub", "", None])
def test_currency_must_be_supported(currency: Any) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload(currency=currency))


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/hook",
        "not-a-url",
        "",
        "http://" + "a" * 2050 + ".com",
        "http://a.com/" + "я" * 400,  # < 2048 символов на входе, > 2048 после percent-encoding
    ],
)
def test_webhook_url_is_validated(url: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload(webhook_url=url))


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload(status="succeeded"))


@pytest.mark.parametrize(
    "metadata",
    [
        [1, 2, 3],
        "string",
        {"blob": "x" * MAX_METADATA_BYTES},
        nested(MAX_METADATA_DEPTH + 1),
        {"x": float("nan")},
        {"x": [float("inf")]},
        {"x": "a\x00b"},
        {"a\x00": 1},
        {"x": "\ud800"},
    ],
)
def test_invalid_metadata_is_rejected(metadata: Any) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload(metadata=metadata))


def test_metadata_is_kept_as_is() -> None:
    metadata = {" key ": 1, "key": 2, "nested": nested(MAX_METADATA_DEPTH - 1), "text": "  spaced  "}
    assert PaymentCreate.model_validate(payload(metadata=metadata)).metadata == metadata


@pytest.mark.parametrize("description", ["a\x00b", "\ud800", "x" * 513])
def test_invalid_description_is_rejected(description: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(payload(description=description))


def test_description_is_stripped_and_blank_becomes_none() -> None:
    assert PaymentCreate.model_validate(payload(description="  Order  ")).description == "Order"
    assert PaymentCreate.model_validate(payload(description="   ")).description is None


def test_optional_fields_have_defaults() -> None:
    model = PaymentCreate.model_validate({"amount": "1", "currency": "USD", "webhook_url": "https://example.com"})
    assert model.description is None
    assert model.metadata == {}


def test_fingerprint_ignores_formatting_differences() -> None:
    a = PaymentCreate.model_validate(payload(amount="100.5", metadata={"a": 1, "b": {"c": 2}}))
    b = PaymentCreate.model_validate(payload(amount=100.50, metadata={"b": {"c": 2}, "a": 1}))
    c = PaymentCreate.model_validate(payload(amount="100.50", metadata={"a": 1, "b": {"c": 2}}))
    assert a.fingerprint() == b.fingerprint() == c.fingerprint()


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "100.51"},
        {"currency": "USD"},
        {"description": "other"},
        {"metadata": {"order_id": 2}},
        {"webhook_url": "https://example.com/other"},
    ],
)
def test_fingerprint_changes_with_payload(change: dict[str, Any]) -> None:
    original = PaymentCreate.model_validate(payload()).fingerprint()
    assert original != PaymentCreate.model_validate(payload(**change)).fingerprint()

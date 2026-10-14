import random
from decimal import Decimal

import pytest

from app.enums import Currency, PaymentStatus
from app.services.gateway import EmulatedPaymentGateway, GatewayError


def make_gateway(success_rate: float = 0.9, error_rate: float = 0.0) -> tuple[EmulatedPaymentGateway, list[float]]:
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    gateway = EmulatedPaymentGateway(
        min_delay=2.0,
        max_delay=5.0,
        success_rate=success_rate,
        error_rate=error_rate,
        rng=random.Random(42),
        sleep=fake_sleep,
    )
    return gateway, delays


async def charge(gateway: EmulatedPaymentGateway, key: str) -> PaymentStatus:
    return await gateway.charge(idempotency_key=key, amount=Decimal("1.00"), currency=Currency.RUB)


async def test_delay_bounds_and_success_rate() -> None:
    gateway, delays = make_gateway()
    results = [await charge(gateway, str(i)) for i in range(2000)]

    assert all(2.0 <= d <= 5.0 for d in delays)
    assert 0.87 < results.count(PaymentStatus.SUCCEEDED) / len(results) < 0.93
    assert set(results) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}


async def test_repeated_call_with_same_key_returns_same_result() -> None:
    gateway, delays = make_gateway(success_rate=0.5)
    first = [await charge(gateway, f"k{i}") for i in range(50)]
    second = [await charge(gateway, f"k{i}") for i in range(50)]

    assert first == second
    assert len(delays) == 50  # повторы отвечают сразу, как настоящий идемпотентный шлюз


async def test_technical_errors_are_raised_and_not_remembered() -> None:
    gateway, _ = make_gateway(error_rate=1.0)
    with pytest.raises(GatewayError):
        await charge(gateway, "k")
    with pytest.raises(GatewayError):
        await charge(gateway, "k")

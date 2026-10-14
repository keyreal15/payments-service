import asyncio
import random
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Protocol

from app.enums import Currency, PaymentStatus


class GatewayError(Exception):
    """Технический сбой шлюза (таймаут, 5xx, недоступность) — платёж не проведён, нужен retry."""


class PaymentGateway(Protocol):
    async def charge(self, *, idempotency_key: str, amount: Decimal, currency: Currency) -> PaymentStatus:
        """Финальный статус платежа. Отказ — это failed, GatewayError — технический сбой.

        Повторный вызов с тем же idempotency_key возвращает прежний результат, а не списывает снова.
        """
        ...


class EmulatedPaymentGateway:
    """Эмуляция шлюза: задержка 2–5 с, 90% успешных. Идемпотентна по ключу (в пределах процесса, LRU)."""

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        error_rate: float = 0.0,
        rng: random.Random | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        max_remembered: int = 100_000,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._error_rate = error_rate
        self._rng = rng or random.Random()  # noqa: S311 - эмуляция, не криптография
        self._sleep = sleep
        self._results: OrderedDict[str, PaymentStatus] = OrderedDict()
        self._max_remembered = max_remembered

    async def charge(self, *, idempotency_key: str, amount: Decimal, currency: Currency) -> PaymentStatus:
        if (known := self._results.get(idempotency_key)) is not None:
            self._results.move_to_end(idempotency_key)
            return known

        await self._sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._error_rate:
            raise GatewayError("Emulated gateway failure")

        status = PaymentStatus.SUCCEEDED if self._rng.random() < self._success_rate else PaymentStatus.FAILED
        self._results[idempotency_key] = status
        if len(self._results) > self._max_remembered:
            self._results.popitem(last=False)
        return status

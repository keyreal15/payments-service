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
        """Проводит платёж и возвращает финальный статус (succeeded/failed).

        Отказ шлюза — бизнес-результат (failed), а не исключение. GatewayError — технический сбой.
        idempotency_key (= payment.id) гарантирует, что повторный вызов после сбоя между ответом
        шлюза и коммитом в нашу БД вернёт тот же результат, а не спишет деньги второй раз.
        """
        ...


class EmulatedPaymentGateway:
    """Эмуляция внешнего шлюза: задержка 2–5 с, 90% успешных платежей.

    Как и настоящий шлюз, идемпотентна по ключу: повторный вызов с тем же ключом
    возвращает сохранённый результат (в пределах процесса, LRU).
    """

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

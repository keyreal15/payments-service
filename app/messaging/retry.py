from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from app.config import Settings

ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"
DEAD_LETTER_REASON_HEADER = "x-dead-letter-reason"
# Служебные заголовки брокера (dead-lettering, счётчики доставок) не переносятся в копии сообщений.
_BROKER_HEADER_PREFIXES = ("x-death", "x-first-death", "x-last-death", "x-delivery-count", "x-acquired-count")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """max_attempts — общее число попыток обработки (первая + повторы).

    После неудачной попытки N (N < max_attempts) сообщение откладывается на
    base_delay * multiplier ** (N - 1) секунд; после последней — уходит в DLQ.
    """

    max_attempts: int
    base_delay: float
    multiplier: float

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay <= 0 or self.multiplier < 1:
            raise ValueError("base_delay must be > 0 and multiplier >= 1")

    def should_retry(self, failed_attempt: int) -> bool:
        return failed_attempt < self.max_attempts

    def delay_for(self, failed_attempt: int) -> float:
        return self.base_delay * self.multiplier ** (failed_attempt - 1)

    def delays_ms(self) -> list[int]:
        """Ступени задержки. Хотя бы одна есть всегда: первая используется и для отсрочки
        без расхода попытки (платёж занят другим обработчиком, недоступна БД)."""
        return [round(self.delay_for(attempt) * 1000) for attempt in range(1, max(self.max_attempts, 2))]


def strip_broker_headers(headers: Mapping[str, Any] | None) -> dict[str, Any]:
    return {key: value for key, value in (headers or {}).items() if not key.startswith(_BROKER_HEADER_PREFIXES)}


def get_attempt(headers: Mapping[str, Any] | None) -> int:
    """Номер текущей попытки; у сообщения от outbox заголовка нет — это попытка №1."""
    raw = (headers or {}).get(ATTEMPT_HEADER)
    try:
        attempt = int(raw) if raw is not None else 1
    except (TypeError, ValueError):
        return 1
    return max(attempt, 1)


def retry_policy_from_settings(settings: Settings) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=settings.retry_max_attempts,
        base_delay=settings.retry_base_delay,
        multiplier=settings.retry_backoff_multiplier,
    )

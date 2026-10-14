import uuid


class PermanentProcessingError(Exception):
    """Ошибка обработки, которую повтор не исправит: сообщение сразу уходит в DLQ."""


class PaymentNotFoundError(PermanentProcessingError):
    def __init__(self, payment_id: uuid.UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")


class WebhookUrlNotAllowedError(PermanentProcessingError):
    """webhook_url указывает на приватный/служебный адрес (защита от SSRF)."""


class TransientProcessingError(Exception):
    """Временная помеха, не связанная с сообщением: обработка откладывается без расхода попытки."""


class PaymentBusyError(TransientProcessingError):
    """Платёж сейчас проводит другой обработчик (его аренда ещё не истекла)."""

    def __init__(self, payment_id: uuid.UUID) -> None:
        super().__init__(f"Payment {payment_id} is being processed by another consumer")


class DatabaseUnavailableError(TransientProcessingError):
    """PostgreSQL недоступна (рестарт, recovery, сеть, исчерпаны соединения)."""


class IdempotencyKeyReusedError(Exception):
    """Ключ идемпотентности уже использован с другим телом запроса."""

    def __init__(self, idempotency_key: str) -> None:
        super().__init__(f"Idempotency-Key {idempotency_key!r} was already used with a different request body")

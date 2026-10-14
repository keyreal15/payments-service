from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Общие настройки api / consumer / утилит. Значения — из переменных окружения (или .env)."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    # --- API ---
    # Обязателен для API (проверяется в create_app); consumer и утилиты обходятся без него.
    api_key: SecretStr | None = None
    api_max_body_size: int = Field(default=64 * 1024, ge=1024)
    docs_enabled: bool = True

    # --- PostgreSQL ---
    database_url: str
    db_pool_size: int = Field(default=10, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)
    db_echo: bool = False

    # --- RabbitMQ ---
    rabbitmq_url: str
    rabbitmq_exchange: str = "payments"
    rabbitmq_queue: str = "payments.new"
    rabbitmq_publish_timeout: float = Field(default=10.0, gt=0)

    # --- Outbox relay ---
    outbox_relay_enabled: bool = True
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, ge=1)
    outbox_retry_base_delay: float = Field(default=1.0, gt=0)
    outbox_retry_max_delay: float = Field(default=30.0, gt=0)

    # --- Consumer ---
    consumer_prefetch_count: int = Field(default=50, ge=1)
    consumer_graceful_timeout: float = Field(default=30.0, gt=0)
    retry_max_attempts: int = Field(default=3, ge=1)
    retry_base_delay: float = Field(default=2.0, gt=0)
    retry_backoff_multiplier: float = Field(default=2.0, ge=1)

    # --- Эмуляция платёжного шлюза ---
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)
    # Доля технических сбоев шлюза (исключение -> retry -> DLQ). По умолчанию 0: неуспешные платежи
    # эмуляции — это отказы шлюза (статус failed). Включается для демонстрации retry ошибок обработки.
    gateway_error_rate: float = Field(default=0.0, ge=0, le=1)
    gateway_timeout: float = Field(default=15.0, gt=0)

    # --- Webhooks ---
    webhook_timeout: float = Field(default=5.0, gt=0)
    webhook_secret: SecretStr | None = None
    # Разрешить webhook на приватные/loopback-адреса (SSRF). Включено только в docker-compose для демо-приёмника.
    webhook_allow_private_networks: bool = False

    @model_validator(mode="after")
    def _check_ranges(self) -> Self:
        if self.gateway_min_delay > self.gateway_max_delay:
            raise ValueError("GATEWAY_MIN_DELAY must be <= GATEWAY_MAX_DELAY")
        if self.gateway_max_delay >= self.gateway_timeout:
            raise ValueError("GATEWAY_MAX_DELAY must be < GATEWAY_TIMEOUT")
        if self.consumer_graceful_timeout < self.gateway_timeout + self.webhook_timeout:
            # Иначе при остановке consumer оборвёт сообщение в работе (оно будет доставлено повторно).
            raise ValueError("CONSUMER_GRACEFUL_TIMEOUT must be >= GATEWAY_TIMEOUT + WEBHOOK_TIMEOUT")
        if self.outbox_retry_base_delay > self.outbox_retry_max_delay:
            raise ValueError("OUTBOX_RETRY_BASE_DELAY must be <= OUTBOX_RETRY_MAX_DELAY")
        if self.api_key is not None and len(self.api_key.get_secret_value().strip()) < 8:
            raise ValueError("API_KEY must be at least 8 non-whitespace characters")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()  # обязательные значения приходят из окружения / .env

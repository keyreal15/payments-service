import os
from typing import Any

from app.config import Settings

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
TEST_RABBITMQ_URL = os.getenv("TEST_RABBITMQ_URL")
TEST_API_KEY = "test-api-key"


def make_settings(**overrides: Any) -> Settings:
    """Настройки для тестов: короткие задержки, чтобы retry/DLQ проверялись за доли секунды."""
    values: dict[str, Any] = {
        "api_key": TEST_API_KEY,
        "database_url": TEST_DATABASE_URL or "postgresql+asyncpg://test:test@localhost:5432/test",
        "rabbitmq_url": TEST_RABBITMQ_URL or "amqp://guest:guest@localhost:5672/",
        "db_pool_size": 5,
        "outbox_poll_interval": 0.05,
        "outbox_retry_base_delay": 0.05,
        "outbox_retry_max_delay": 0.2,
        "retry_max_attempts": 3,
        "retry_base_delay": 0.2,
        "retry_backoff_multiplier": 2.0,
        "gateway_min_delay": 0.0,
        "gateway_max_delay": 0.01,
        "gateway_success_rate": 1.0,
        "gateway_timeout": 2.0,
        "webhook_timeout": 2.0,
        # webhook в тестах уходит в httpx.MockTransport на несуществующий хост — проверку SSRF
        # покрывают отдельные unit-тесты WebhookSender.
        "webhook_allow_private_networks": True,
        "consumer_graceful_timeout": 5.0,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)

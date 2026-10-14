from typing import Any

import pytest
from pydantic import ValidationError

from app.api.app import create_app
from tests.settings import make_settings


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"gateway_min_delay": 5, "gateway_max_delay": 2}, "GATEWAY_MIN_DELAY"),
        ({"gateway_max_delay": 10, "gateway_timeout": 10}, "GATEWAY_TIMEOUT"),
        ({"outbox_retry_base_delay": 10, "outbox_retry_max_delay": 1}, "OUTBOX_RETRY_BASE_DELAY"),
        ({"api_key": "   short "}, "API_KEY"),
        ({"log_level": "verbose"}, "log_level"),
        ({"consumer_graceful_timeout": 5, "gateway_timeout": 4, "webhook_timeout": 2}, "CONSUMER_GRACEFUL_TIMEOUT"),
    ],
)
def test_settings_validation(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        make_settings(**overrides)


def test_api_requires_api_key() -> None:
    with pytest.raises(RuntimeError, match="API_KEY"):
        create_app(make_settings(api_key=None))


def test_docs_can_be_disabled() -> None:
    app = create_app(make_settings(docs_enabled=False))
    assert app.openapi_url is None
    assert app.docs_url is None


def test_openapi_documents_unified_error_format() -> None:
    schema = create_app(make_settings()).openapi()
    for path, method in [("/api/v1/payments", "post"), ("/api/v1/payments/{payment_id}", "get")]:
        responses = schema["paths"][path][method]["responses"]
        assert "ValidationErrorResponse" in str(responses["422"])
        assert "ErrorResponse" in str(responses["401"])
    assert "Idempotent-Replayed" in schema["paths"]["/api/v1/payments"]["post"]["responses"]["202"]["headers"]
    assert schema["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"

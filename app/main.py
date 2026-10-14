"""Точка входа API: `uvicorn app.main:app`."""

from app.api.app import create_app
from app.config import get_settings
from app.logging_config import configure_logging

configure_logging(get_settings().log_level)
app = create_app()

import asyncpg
import pytest
from sqlalchemy import exc as sa_exc

from app.db.errors import is_database_unavailable


def wrapped(driver_error: Exception, sa_class: type[sa_exc.DBAPIError] = sa_exc.DBAPIError) -> Exception:
    """Как SQLAlchemy оборачивает ошибку драйвера при выполнении запроса."""
    try:
        try:
            raise driver_error
        except Exception as exc:
            raise sa_class("SELECT 1", {}, exc) from exc
    except Exception as outer:
        return outer


@pytest.mark.parametrize(
    "error",
    [
        # Ошибки установки соединения asyncpg приходят «сырыми»
        asyncpg.CannotConnectNowError("the database system is starting up"),
        asyncpg.TooManyConnectionsError("too many connections"),
        asyncpg.AdminShutdownError("terminating connection due to administrator command"),
        asyncpg.ConnectionDoesNotExistError("connection was closed in the middle of operation"),
        ConnectionRefusedError("connection refused"),
        TimeoutError(),  # таймаут подключения
        OSError("Name or service not known"),
        sa_exc.TimeoutError("QueuePool limit reached"),
        # Те же ошибки посреди запроса — обёрнутые SQLAlchemy
        wrapped(asyncpg.AdminShutdownError("terminating")),
        wrapped(ConnectionResetError("reset by peer")),
    ],
)
def test_unavailability_is_detected(error: Exception) -> None:
    assert is_database_unavailable(error)


def test_invalidated_connection_is_unavailability() -> None:
    error = sa_exc.DBAPIError("SELECT 1", {}, Exception("closed"), connection_invalidated=True)
    assert is_database_unavailable(error)


@pytest.mark.parametrize(
    "error",
    [
        wrapped(asyncpg.UniqueViolationError("duplicate key"), sa_exc.IntegrityError),
        wrapped(asyncpg.CheckViolationError("violates check constraint"), sa_exc.IntegrityError),
        wrapped(asyncpg.UndefinedTableError("relation does not exist"), sa_exc.ProgrammingError),
        wrapped(asyncpg.DataError("invalid input"), sa_exc.DBAPIError),
        ValueError("bug"),
        KeyError("bug"),
    ],
)
def test_data_and_logic_errors_are_not_unavailability(error: Exception) -> None:
    # Иначе сообщение откладывалось бы бесконечно, не дойдя до DLQ.
    assert not is_database_unavailable(error)

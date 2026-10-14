"""Распознавание недоступности PostgreSQL (в отличие от ошибок в данных или запросе)."""

import asyncpg
from sqlalchemy import exc as sa_exc

# Ошибки подключения asyncpg SQLAlchemy не оборачивает: при установке соединения они приходят «сырыми».
_UNAVAILABLE: tuple[type[BaseException], ...] = (
    asyncpg.PostgresConnectionError,  # SQLSTATE 08xxx, в т. ч. ConnectionDoesNotExistError
    asyncpg.CannotConnectNowError,  # 57P03: сервер стартует / останавливается / в recovery
    asyncpg.AdminShutdownError,  # 57P01
    asyncpg.CrashShutdownError,  # 57P02
    asyncpg.TooManyConnectionsError,  # 53300
    sa_exc.TimeoutError,  # таймаут ожидания соединения из пула
    OSError,  # ConnectionError, socket.gaierror, TimeoutError подключения
)


def _causes(error: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = error
    while current is not None and current not in chain:
        chain.append(current)
        current = current.__cause__ or (None if current.__suppress_context__ else current.__context__)
    return chain


def is_database_unavailable(error: BaseException) -> bool:
    """True для сетевых сбоев, таймаутов подключения, рестарта/recovery сервера, исчерпания соединений.

    False для ошибок данных и запросов (IntegrityError, DataError, ProgrammingError и т. п.) —
    их повтор не исправит, и такие сообщения не должны крутиться бесконечно.
    """
    for cause in _causes(error):
        if isinstance(cause, sa_exc.DBAPIError):
            if cause.connection_invalidated:
                return True
            continue  # решает исходная ошибка драйвера (она в цепочке __cause__)
        if isinstance(cause, _UNAVAILABLE):
            return True
    return False

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings

SessionFactory = async_sessionmaker[AsyncSession]


def create_db_engine(
    settings: Settings,
    *,
    application_name: str,
    min_pool_size: int = 0,
    idle_in_transaction_timeout: float | None = None,
) -> AsyncEngine:
    server_settings = {"application_name": application_name}
    if idle_in_transaction_timeout is not None:
        # PostgreSQL сам оборвёт сессию, «зависшую» в открытой транзакции (и снимет её блокировки),
        # если процесс-владелец умер без закрытия TCP-соединения.
        server_settings["idle_in_transaction_session_timeout"] = str(int(idle_in_transaction_timeout * 1000))
    return create_async_engine(
        settings.database_url,
        echo=settings.db_echo,
        pool_size=max(settings.db_pool_size, min_pool_size),
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        hide_parameters=True,  # значения (webhook_url, metadata) не попадают в тексты ошибок и логи
        connect_args={"server_settings": server_settings},
    )


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    return async_sessionmaker(engine, expire_on_commit=False)

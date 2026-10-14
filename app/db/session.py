from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings

SessionFactory = async_sessionmaker[AsyncSession]


def create_db_engine(settings: Settings, *, application_name: str) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        hide_parameters=True,  # значения (webhook_url, metadata) не попадают в тексты ошибок и логи
        connect_args={"server_settings": {"application_name": application_name}},
    )


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    return async_sessionmaker(engine, expire_on_commit=False)

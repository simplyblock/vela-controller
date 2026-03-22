import os
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from .api.settings import get_settings
from .models import events as _  # noqa: F401 — registers SQLAlchemy event listeners


def _is_worker() -> bool:
    return bool(os.environ.get("VELA_BROKER_URL"))


if _is_worker():
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(str(get_settings().postgres_url), poolclass=NullPool)
else:
    engine = create_async_engine(
        str(get_settings().postgres_url),
        pool_pre_ping=True,
        pool_recycle=3600,
    )

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db() -> AsyncGenerator:
    async with AsyncSessionLocal() as session:
        yield session


async def _get_session():
    async with AsyncSession(engine) as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(_get_session)]

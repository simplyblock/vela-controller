from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from .settings import get_settings

# Enable `pool_pre_ping` and periodic recycling so ASGI workers notice connections
# that Postgres closed while idle (seen as "connection is closed" during requests)
# and transparently reopen them before handing out a session.
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

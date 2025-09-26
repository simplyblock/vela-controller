from typing import Annotated, Any
from uuid import UUID

from fastapi import Depends
from sqlalchemy import UUID as SQLAlchemyUUID  # noqa: N811
from sqlalchemy import TypeDecorator
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession
from ulid import ULID

from .settings import settings

# Enable `pool_pre_ping` and periodic recycling so ASGI workers notice connections
# that Postgres closed while idle (seen as "connection is closed" during requests)
# and transparently reopen them before handing out a session.
engine = create_async_engine(
    str(settings.postgres_url),
    pool_pre_ping=True,
    pool_recycle=3600,
)


async def _get_session():
    async with AsyncSession(engine) as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(_get_session)]


class DBULID(TypeDecorator):
    """SQLAlchemy type that stores ULIDs as UUIDs in the database."""

    impl = SQLAlchemyUUID
    cache_ok = True

    def process_bind_param(self, value: Any, _dialect) -> UUID | None:
        """Convert ULID string to UUID for database storage."""
        if value is None:
            return value
        if not isinstance(value, ULID):
            raise TypeError(type(value), value)
        return UUID(bytes=value.bytes)

    def process_result_value(self, value: Any, _dialect) -> ULID | None:
        """Convert UUID from database back to ULID string."""
        if value is None:
            return value
        if not isinstance(value, UUID):
            raise TypeError(type(value), value)
        return ULID.from_bytes(value.bytes)

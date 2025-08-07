import os
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

engine = create_async_engine(os.environ['POSTGRES_URL'])


async def _get_session():
    async with AsyncSession(engine) as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(_get_session)]

import contextlib

from fastapi import FastAPI
from fastapi.routing import APIRoute
from sqlmodel import SQLModel

from . import models
from .api import router, users
from .db import engine


async def _create_db_and_tables():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
        await conn.run_sync(models.user.Base.metadata.create_all)


async def _create_sample_user(user_db: users.UserDBDep):
    async with contextlib.asynccontextmanager(users.get_user_manager)(user_db) as user_manager:
        user = await user_manager.create(
            models.user.UserCreate(email='foo@bar.baz', password='password', is_superuser=True),
        )
        print(f"User created {user}")
        return user


def _use_route_names_as_operation_ids(app: FastAPI) -> None:
    """
    Simplify operation IDs so that generated API clients have simpler function
    names.

    Should be called only after all routes have been added.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            route.operation_id = route.name


app = FastAPI()
users.register(app)


@app.on_event("startup")
async def on_startup():
    await _create_db_and_tables()


app.include_router(router)
_use_route_names_as_operation_ids(app)


__all__ = ['app', 'models']


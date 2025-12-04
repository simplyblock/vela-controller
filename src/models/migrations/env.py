from alembic import context
from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict
from simplyblock.vela import models  # noqa: F401
from sqlalchemy import create_engine
from sqlmodel import SQLModel


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)

    postgres_url: PostgresDsn


settings = Settings()  # type: ignore[call-arg]


def render_item(_type, obj, _autogen_context):
    """Render custom database identifiers as UUID."""

    if obj.__class__.__name__ == "DatabaseIdentifier":
        return "sa.UUID(as_uuid=True)"

    return False  # Use default rendering


def run_migrations_offline() -> None:
    context.configure(
        url=str(settings.postgres_url).replace("+asyncpg", "+psycopg"),
        target_metadata=SQLModel.metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(str(settings.postgres_url).replace("+asyncpg", "+psycopg"))

    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=SQLModel.metadata, render_item=render_item)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import UUID as SQLAlchemyUUID  # noqa: N811
from sqlalchemy import TypeDecorator
from sqlmodel import Field as SQLField
from sqlmodel import SQLModel
from ulid import ULID

from ..._util import Identifier


class _DatabaseIdentifier(TypeDecorator):
    """SQLAlchemy type that stores ULIDs as UUIDs in the database."""

    impl = SQLAlchemyUUID
    cache_ok = True

    def process_bind_param(self, value: Any, _dialect) -> UUID | None:
        """Convert ULID string to UUID for database storage."""
        if value is None:
            return value
        if not isinstance(value, ULID):
            raise TypeError(type(value), value)
        return value.to_uuid()

    def process_result_value(self, value: Any, _dialect) -> ULID | None:
        """Convert UUID from database back to ULID string."""
        if value is None:
            return value
        if not isinstance(value, UUID):
            raise TypeError(type(value), value)
        return ULID.from_bytes(value.bytes)


class Model(SQLModel):
    id: Identifier = SQLField(default_factory=ULID, primary_key=True, sa_type=_DatabaseIdentifier)

    # This would ideally be a classmethod, but initialization order prevents that
    @staticmethod
    def foreign_key_field(table_name, *, nullable=False, **kwargs):
        return SQLField(
            default=None if nullable else ...,
            foreign_key=f"{table_name}.id",
            sa_type=_DatabaseIdentifier,
            **kwargs,
        )

    @property
    def created_datetime(self) -> datetime:
        """Return the timestamp encoded in this model's ULID identifier.

        ULIDs carry millisecond precision timestamps; we treat them as UTC-aware datetimes.
        """

        ulid: ULID = self.id
        dt = ulid.datetime
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

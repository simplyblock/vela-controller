from typing import Annotated

from pydantic import AfterValidator, StringConstraints
from pydantic import Field as PDField
from slugify import slugify
from sqlalchemy import BigInteger
from sqlmodel import Field as SQLField
from sqlmodel import SQLModel

from ..._util import dbstr


def update_slug(mapper, connection, target):  # noqa
    target.slug = slugify(target.name, max_length=50)


def _validate_sluggable(string: str):
    if len(slugify(string, max_length=50)) == 0:
        raise ValueError("Derived slug is empty")
    return string


Name = Annotated[
    dbstr,
    StringConstraints(
        min_length=1,
    ),
    AfterValidator(_validate_sluggable),
]


Identifier = Annotated[int, PDField(ge=-(2**63), lt=2**63)]
_DatabaseIdentifier = BigInteger


class Model(SQLModel):
    id: Identifier | None = SQLField(primary_key=True, sa_type=_DatabaseIdentifier)

    # This would ideally be a classmethod, but initialization order prevents that
    @staticmethod
    def foreign_key_field(table_name, *, nullable=False, **kwargs):
        return SQLField(
            default=None if nullable else ...,
            foreign_key=f"{table_name}.id",
            sa_type=_DatabaseIdentifier,
            **kwargs,
        )

    def dbid(self) -> Identifier:
        if self.id is None:
            raise ValueError("Model not tracked in database")
        return self.id

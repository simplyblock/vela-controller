from typing import Annotated

from pydantic import StringConstraints
from sqlmodel import Field, SQLModel
from ulid import ULID

from ..._util import dbstr
from .._util import ULIDType
from ..db import DBULID

Name = Annotated[
    dbstr,
    StringConstraints(
        min_length=1,
    ),
]


class Model(SQLModel):
    id: ULIDType = Field(default_factory=ULID, primary_key=True, sa_type=DBULID)

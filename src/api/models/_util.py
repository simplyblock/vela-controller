from typing import Annotated

import ulid
from pydantic import StringConstraints

from ..._util import dbstr

_MAX_LENGTH = 50

Slug = Annotated[
    str,
    StringConstraints(
        pattern=r"^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$",
        min_length=1,
        max_length=_MAX_LENGTH,
    ),
]


def update_slug(mapper, connection, target):  # noqa
    target.slug = str(ulid.ULID())


Name = Annotated[
    dbstr,
    StringConstraints(
        min_length=1,
    )
]

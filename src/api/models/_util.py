from typing import Annotated

import ulid
from pydantic import StringConstraints

from ..._util import dbstr


def update_slug(mapper, connection, target):  # noqa
    target.slug = str(ulid.ULID())


Name = Annotated[
    dbstr,
    StringConstraints(
        min_length=1,
    ),
]

from typing import Annotated

from pydantic import StringConstraints

from ..._util import dbstr

Name = Annotated[
    dbstr,
    StringConstraints(
        min_length=1,
    ),
]

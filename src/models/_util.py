from typing import Annotated

from pydantic import StringConstraints

Slug = Annotated[str, StringConstraints(
        pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$',
        max_length=50,
)]

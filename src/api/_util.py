from typing import Annotated

from pydantic import Field

Int64 = Annotated[int, Field(ge=-2 ** 63, lt=2 ** 63)]

from typing import Annotated, Any

import fdb
from fastapi import Depends


def _get_db():
    yield fdb.open()


DB = Annotated[Any, Depends(_get_db)]

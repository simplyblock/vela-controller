from typing import Annotated

from pydantic import AfterValidator, StringConstraints
from slugify import slugify

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

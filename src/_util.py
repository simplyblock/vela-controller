from typing import Annotated

from pydantic import Field, StringConstraints

Slug = Annotated[str, StringConstraints(
        pattern=r'^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$',
        max_length=50,
)]

Int64 = Annotated[int, Field(ge=-2 ** 63, lt=2 ** 63)]


def single(xs):
    """Returns the single value in the passed collection

    If `xs` contains zero or multiple values, a ValueError error is raised.
    """

    it = iter(xs)

    try:
        x = next(it)
    except StopIteration:
        raise ValueError('No values present') from None

    try:
        next(it)
        raise ValueError('Multiple values present')
    except StopIteration:
        return x

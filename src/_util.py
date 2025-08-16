from typing import Annotated

from pydantic import Field


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


dbstr = Annotated[str, Field(pattern=r'^[^\x00]*$')]

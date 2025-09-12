import asyncio
import subprocess
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
        raise ValueError("No values present") from None

    try:
        next(it)
        raise ValueError("Multiple values present")
    except StopIteration:
        return x


dbstr = Annotated[str, Field(pattern=r"^[^\x00]*$")]


async def check_output(cmd: list[str], *, stderr=None, text: bool = False, timeout: float | None = None):
    process = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE)

    raw_stdout, raw_stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    returncode = await process.wait()

    stdout = raw_stdout if not text else raw_stdout.decode()
    stderr = raw_stderr if ((raw_stderr is None) or not text) else raw_stderr.decode()

    if returncode != 0:
        raise subprocess.CalledProcessError(
            returncode,
            cmd,
            output=stdout,
            stderr=stderr,
        )

    return stdout

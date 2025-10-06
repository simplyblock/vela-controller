import asyncio
import subprocess
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Final, Literal

from pydantic import BeforeValidator, Field, PlainSerializer, StringConstraints, WithJsonSchema
from ulid import ULID

_MAX_LENGTH = 50

KIB: Final[int] = 1000
MIB: Final[int] = KIB * 1000
GIB: Final[int] = MIB * 1000

Slug = Annotated[
    str,
    StringConstraints(
        pattern=r"^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$",
        min_length=1,
        max_length=_MAX_LENGTH,
    ),
]

# Represents the state of Kubevirt VM
# https://github.com/kubevirt/kubevirt/blob/main/staging/src/kubevirt.io/api/core/v1/types.go#L1897-L1942
StatusType = Literal[
    "Stopped",
    "Provisioning",
    "Starting",
    "Running",
    "Paused",
    "Stopping",
    "Terminating",
    "CrashLoopBackOff",
    "Migrating",
    "Unknown",
    "ErrorUnschedulable",
    "ErrImagePull",
    "ImagePullBackOff",
    "ErrorPvcNotFound",
    "DataVolumeError",
    "WaitingForVolumeBinding",
    "WaitingForReceiver",
    "UNKNOWN",
]


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


def validate_ulid(v: Any) -> ULID:
    if isinstance(v, ULID):
        return v
    if isinstance(v, str):
        return ULID.from_str(v)
    raise ValueError("Invalid ULID format")


Identifier = Annotated[
    ULID,
    BeforeValidator(validate_ulid),
    PlainSerializer(lambda ulid: str(ulid), return_type=str),
    WithJsonSchema(
        {
            "type": "string",
            "format": "ulid",
            "pattern": r"^[0-7][0-9A-HJKMNP-TV-Z]{25}$",
            "minLength": 26,
            "maxLength": 26,
            "description": "A ULID (Universally Unique Lexicographically Sortable Identifier)",
            "examples": ["01ARZ3NDEKTSV4RRFFQ69G5FAV", "01H945P9C3K2QJ8F7N6M4R2E8V"],
            "title": "ULID",
        }
    ),
]


def bytes_to_kib(value: int) -> int:
    """Convert a byte count to the nearest whole KiB using floor division."""

    return value // KIB


def bytes_to_mib(value: int) -> int:
    """Convert a byte count to the nearest whole MiB using floor division."""

    return value // MIB


def bytes_to_gib(value: int) -> int:
    """Convert a byte count to the nearest whole GiB using floor division."""

    return value // GIB


def kib_to_bytes(value: int) -> int:
    """Convert a KiB count to bytes."""

    return value * KIB


def mib_to_bytes(value: int) -> int:
    """Convert a MiB count to bytes."""

    return value * MIB


def gib_to_bytes(value: int | float) -> int:
    """Convert a GiB count to bytes, accepting integer or fractional sizes."""

    decimal_value = Decimal(str(value))
    bytes_decimal = decimal_value * Decimal(GIB)
    # Align to the nearest KiB so downstream validations expecting multiples of KiB still pass.
    kib_units = (bytes_decimal / Decimal(KIB)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return int(kib_units * Decimal(KIB))

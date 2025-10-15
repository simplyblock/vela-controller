import asyncio
import subprocess
from typing import Annotated, Any, Final, Literal

from pydantic import BeforeValidator, Field, PlainSerializer, StringConstraints, WithJsonSchema
from ulid import ULID

_MAX_LENGTH = 50

KB: Final[int] = 1000
MB: Final[int] = KB * 1000
GB: Final[int] = MB * 1000
TB: Final[int] = GB * 1000

# FIXME: Increasing to have faster boot times.
# Reduce min vcpu and memory when when the image is optimised and boot time is improved.
VCPU_MILLIS_MIN = 2000  # in milli vCPU
VCPU_MILLIS_MAX = 64000
VCPU_MILLIS_STEP = 100

MEMORY_MIN = 4 * GB
MEMORY_MAX = 256 * GB
MEMORY_STEP = 100 * MB

DB_SIZE_MIN = 1 * GB
DB_SIZE_MAX = 100 * TB
DB_SIZE_STEP = GB

STORAGE_SIZE_MIN = 500 * MB
STORAGE_SIZE_MAX = 1 * TB
STORAGE_SIZE_STEP = GB

IOPS_MIN = 100
IOPS_MAX = 2**31 - 1


CPU_CONSTRAINTS = {"ge": VCPU_MILLIS_MIN, "le": VCPU_MILLIS_MAX, "multiple_of": VCPU_MILLIS_STEP}
MEMORY_CONSTRAINTS = {"ge": MEMORY_MIN, "le": MEMORY_MAX, "multiple_of": MEMORY_STEP}
DATABASE_SIZE_CONSTRAINTS = {"ge": DB_SIZE_MIN, "le": DB_SIZE_MAX, "multiple_of": DB_SIZE_STEP}
STORAGE_SIZE_CONSTRAINTS = {"ge": STORAGE_SIZE_MIN, "le": STORAGE_SIZE_MAX, "multiple_of": STORAGE_SIZE_STEP}
IOPS_CONSTRAINTS = {"ge": IOPS_MIN, "le": IOPS_MAX}

Slug = Annotated[
    str,
    StringConstraints(
        pattern=r"^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$",
        min_length=1,
        max_length=_MAX_LENGTH,
    ),
]

DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"

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

DBPassword = Annotated[
    dbstr,
    StringConstraints(
        min_length=8,
        max_length=128,
    ),
]


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


def bytes_to_kb(value: int) -> int:
    """Convert a byte count to the nearest whole KB using floor division."""

    return value // KB


def bytes_to_mb(value: int) -> int:
    """Convert a byte count to the nearest whole MB using floor division."""

    return value // MB


def bytes_to_gb(value: int) -> int:
    """Convert a byte count to the nearest whole GB using floor division."""

    return value // GB


def kb_to_bytes(value: int) -> int:
    """Convert a KB count to bytes."""

    return value * KB


def mb_to_bytes(value: int) -> int:
    """Convert a MB count to bytes."""

    return value * MB

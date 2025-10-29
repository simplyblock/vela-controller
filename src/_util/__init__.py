import asyncio
import subprocess
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Final, Literal

from pydantic import BeforeValidator, Field, PlainSerializer, StringConstraints, WithJsonSchema
from ulid import ULID

_MAX_LENGTH = 50

KB: Final[int] = 1000
MB: Final[int] = KB * 1000
GB: Final[int] = MB * 1000
TB: Final[int] = GB * 1000

KIB: Final[int] = 1024
MIB: Final[int] = KIB * 1024
GIB: Final[int] = MIB * 1024
TIB: Final[int] = GIB * 1024

PGBOUNCER_DEFAULT_MAX_CLIENT_CONN: Final[int] = 100
PGBOUNCER_DEFAULT_POOL_SIZE: Final[int] = 20
PGBOUNCER_DEFAULT_SERVER_IDLE_TIMEOUT: Final[int] = 60
PGBOUNCER_DEFAULT_SERVER_LIFETIME: Final[int] = 600
PGBOUNCER_DEFAULT_QUERY_WAIT_TIMEOUT: Final[int] = 30
PGBOUNCER_DEFAULT_RESERVE_POOL_SIZE: Final[int] = 0


# FIXME: Increasing to have faster boot times.
# Reduce min vcpu and memory when when the image is optimised and boot time is improved.
VCPU_MILLIS_MIN = 2000  # in milli vCPU
VCPU_MILLIS_MAX = 64000
VCPU_MILLIS_STEP = 100

MEMORY_MIN = 2 * GIB
MEMORY_MAX = 256 * GIB
MEMORY_STEP = 128 * MIB  # 12.5% of 1 GiB

DB_SIZE_MIN = 1 * GB
DB_SIZE_MAX = 100 * TB
DB_SIZE_STEP = GB

STORAGE_SIZE_MIN = 500 * MB
STORAGE_SIZE_MAX = 1 * TB
STORAGE_SIZE_STEP = GB

IOPS_MIN = 100
IOPS_MAX = 2**31 - 1
IOPS_STEP = 100


CPU_CONSTRAINTS = {"ge": VCPU_MILLIS_MIN, "le": VCPU_MILLIS_MAX, "multiple_of": VCPU_MILLIS_STEP}
MEMORY_CONSTRAINTS = {"ge": MEMORY_MIN, "le": MEMORY_MAX, "multiple_of": MEMORY_STEP}
DATABASE_SIZE_CONSTRAINTS = {"ge": DB_SIZE_MIN, "le": DB_SIZE_MAX, "multiple_of": DB_SIZE_STEP}
STORAGE_SIZE_CONSTRAINTS = {"ge": STORAGE_SIZE_MIN, "le": STORAGE_SIZE_MAX, "multiple_of": STORAGE_SIZE_STEP}
IOPS_CONSTRAINTS = {"ge": IOPS_MIN, "le": IOPS_MAX}
_QUANTITY_SUFFIXES: dict[str, int] = {
    "ki": KIB,
    "mi": MIB,
    "gi": GIB,
    "ti": TIB,
    "k": KB,
    "m": MB,
    "g": GB,
    "t": TB,
}

_CPU_QUANTITY_FACTORS: dict[str, Decimal] = {
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "": Decimal("1"),
    "k": Decimal("1e3"),
    "K": Decimal("1e3"),
    "M": Decimal("1e6"),
    "G": Decimal("1e9"),
    "T": Decimal("1e12"),
    "P": Decimal("1e15"),
    "E": Decimal("1e18"),
}

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

Name = Annotated[
    dbstr,
    StringConstraints(
        min_length=1,
    ),
]

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


def bytes_to_mib(value: int) -> int:
    """Convert a byte count to the nearest whole MiB using floor division."""

    return value // MIB


def bytes_to_gb(value: int) -> int:
    """Convert a byte count to the nearest whole GB using floor division."""

    return value // GB


def bytes_to_gib(value: int) -> int:
    """Convert a byte count to the nearest whole GiB using floor division."""

    return value // GIB


def kb_to_bytes(value: int) -> int:
    """Convert a KB count to bytes."""

    return value * KB


def mb_to_bytes(value: int) -> int:
    """Convert a MB count to bytes."""

    return value * MB


def quantity_to_milli_cpu(value: str | None) -> int | None:
    """Convert a CPU quantity (e.g. '250m', '62105876n') to milli vCPU units."""

    if value is None:
        return None

    quantity = value.strip()
    if not quantity:
        return None

    suffix = ""
    number = quantity
    if quantity[-1].isalpha():
        candidate = quantity[-1]
        if candidate in _CPU_QUANTITY_FACTORS:
            suffix = candidate
            number = quantity[:-1]
        else:
            # Unsupported suffix; fall back to parsing the entire value.
            suffix = ""
            number = quantity

    try:
        numeric = Decimal(number)
    except (InvalidOperation, ValueError):
        return None

    try:
        factor = _CPU_QUANTITY_FACTORS[suffix]
    except KeyError:
        return None

    milli_value = numeric * factor * Decimal(1000)
    try:
        return int(milli_value)
    except (ValueError, OverflowError):
        return None


def quantity_to_bytes(value: str | None) -> int | None:
    """Convert a Kubernetes-style quantity string (e.g. '10Gi', '512Mi') to bytes.

    Returns ``None`` for empty values and logs no errors, leaving caller responsible for handling
    unexpected formats.
    """

    if value is None:
        return None

    quantity = value.strip()
    if not quantity:
        return None

    for suffix, factor in _QUANTITY_SUFFIXES.items():
        if quantity.lower().endswith(suffix):
            number = quantity[: -len(suffix)]
            try:
                return int(Decimal(number) * factor)
            except (InvalidOperation, ValueError):
                return None

    try:
        return int(Decimal(quantity))
    except (InvalidOperation, ValueError):
        return None

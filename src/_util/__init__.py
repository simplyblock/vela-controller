import asyncio
import subprocess
from decimal import Decimal
from typing import Annotated, Any, Final, Literal

from kubernetes.utils import parse_quantity
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
VCPU_MILLIS_MIN = 500  # in milli vCPU
VCPU_MILLIS_MAX = 64000
VCPU_MILLIS_STEP = 100

MEMORY_MIN = 1 * GIB
MEMORY_MAX = 32 * GIB
MEMORY_STEP = 256 * MIB
AUTOSCALER_MEMORY_SLOT_SIZE_MIB = MEMORY_STEP // MIB
AUTOSCALER_MEMORY_SLOTS_MIN = MEMORY_MIN // AUTOSCALER_MEMORY_SLOT_SIZE_MIB
AUTOSCALER_MEMORY_SLOTS_MAX = MEMORY_MAX // AUTOSCALER_MEMORY_SLOT_SIZE_MIB

DB_SIZE_MIN = 1 * GB
DB_SIZE_MAX = 100 * TB
DB_SIZE_STEP = GB

STORAGE_SIZE_MIN = 1 * GB
STORAGE_SIZE_MAX = 1 * TB
STORAGE_SIZE_STEP = GB

IOPS_MIN = 100
IOPS_MAX = 100000
IOPS_STEP = 100


CPU_CONSTRAINTS = {"ge": VCPU_MILLIS_MIN, "le": VCPU_MILLIS_MAX, "multiple_of": VCPU_MILLIS_STEP}
MEMORY_CONSTRAINTS = {"ge": MEMORY_MIN, "le": MEMORY_MAX, "multiple_of": MEMORY_STEP}
DATABASE_SIZE_CONSTRAINTS = {"ge": DB_SIZE_MIN, "le": DB_SIZE_MAX, "multiple_of": DB_SIZE_STEP}
STORAGE_SIZE_CONSTRAINTS = {"ge": STORAGE_SIZE_MIN, "le": STORAGE_SIZE_MAX, "multiple_of": STORAGE_SIZE_STEP}
IOPS_CONSTRAINTS = {"ge": IOPS_MIN, "le": IOPS_MAX}

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

Quantity = Annotated[Decimal, BeforeValidator(parse_quantity)]


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


def _normalize_quantity(value: str | Decimal | None) -> Decimal | None:
    """Return the parsed decimal quantity or ``None`` when the input is empty."""

    if value is None:
        return None

    if isinstance(value, Decimal):
        return value

    quantity = value.strip()
    if not quantity:
        return None

    return parse_quantity(quantity)


def quantity_to_milli_cpu(value: str | Decimal | None) -> int | None:
    """Convert a CPU quantity (e.g. '250m', '62105876n') to milli vCPU units."""

    quantity = _normalize_quantity(value)
    if quantity is None:
        return None

    return int(quantity * Decimal(1000))


def quantity_to_bytes(value: str | Decimal | None) -> int | None:
    """Convert a Kubernetes-style quantity string (e.g. '10Gi', '512Mi') to bytes."""

    quantity = _normalize_quantity(value)
    if quantity is None:
        return None

    return int(quantity)


def permissive_numeric_timedelta(value: Any) -> Any:
    """Parses the given value into timedelta

    Defers to base-pydantic handling, but parses the value into int or float if
    applicable, s.t. they are interpreted as seconds.
    """
    if isinstance(value, str):
        try:
            return int(value)
            return float(value)
        except ValueError:
            pass

    return value

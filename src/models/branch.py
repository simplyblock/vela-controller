import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, Optional, cast

from pydantic import BaseModel, ConfigDict, ValidationError, model_validator
from pydantic import Field as PydanticField
from sqlalchemy import BigInteger, Boolean, Column, String, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlmodel import Field, Relationship

from .._util import (
    CPU_CONSTRAINTS,
    DATABASE_SIZE_CONSTRAINTS,
    IOPS_CONSTRAINTS,
    MEMORY_CONSTRAINTS,
    PGBOUNCER_DEFAULT_MAX_CLIENT_CONN,
    PGBOUNCER_DEFAULT_POOL_SIZE,
    PGBOUNCER_DEFAULT_QUERY_WAIT_TIMEOUT,
    PGBOUNCER_DEFAULT_RESERVE_POOL_SIZE,
    PGBOUNCER_DEFAULT_SERVER_IDLE_TIMEOUT,
    PGBOUNCER_DEFAULT_SERVER_LIFETIME,
    STORAGE_SIZE_CONSTRAINTS,
    DBPassword,
    Identifier,
    Name,
)
from .._util.crypto import (
    decrypt_with_base64_key,
    decrypt_with_passphrase,
    encrypt_with_passphrase,
    encrypt_with_random_passphrase,
)
from ..deployment import DeploymentParameters
from ._util import DateTimeTZ, Model
from .project import Project

if TYPE_CHECKING:
    from .backups import BackupEntry, BackupLog, BackupSchedule, NextBackup


def _default_resource_usage_payload() -> dict[str, Any]:
    return {
        "milli_vcpu": 0,
        "ram_bytes": 0,
        "nvme_bytes": 0,
        "iops": 0,
        "storage_bytes": None,
    }


class Branch(AsyncAttrs, Model, table=True):
    DEFAULT_SLUG: ClassVar[Name] = "main"

    def __init__(self, **data: Any):
        status_value = data.get("status")
        status_timestamp = data.get("status_updated_at")
        super().__init__(**data)
        if status_value is not None and status_timestamp is None:
            # Force timestamp on initial construction when a status is provided.
            self.set_status(status_value, force_timestamp=True)

    name: Name
    env_type: str | None = Field(default=None, sa_column=Column(String(255), nullable=True))
    project_id: Identifier = Model.foreign_key_field("project")
    project: Project | None = Relationship(back_populates="branches")
    parent_id: Identifier | None = Model.foreign_key_field("branch", nullable=True, ondelete="SET NULL")
    parent: Optional["Branch"] = Relationship()
    endpoint_domain: str | None = Field(default=None, sa_column=Column(String(255), nullable=True))
    backup_schedules: list["BackupSchedule"] = Relationship(back_populates="branch", cascade_delete=True)
    next_backups: list["NextBackup"] = Relationship(back_populates="branch", cascade_delete=True)
    backup_entries: list["BackupEntry"] = Relationship(back_populates="branch", cascade_delete=True)
    backup_logs: list["BackupLog"] = Relationship(back_populates="branch", cascade_delete=True)
    api_keys: list["BranchApiKey"] = Relationship(back_populates="branch", cascade_delete=True)

    # Deployment parameters specific to this branch
    database: Annotated[str, Field(sa_column=Column(String(255)))]
    database_user: Annotated[str, Field(sa_column=Column(String(255)))]

    # base64-encoded encrypted password and encryption key
    encrypted_database_password: Annotated[str, Field(default="", sa_column=Column(Text, nullable=False))]
    encryption_key: Annotated[str, Field(default="", sa_column=Column(String(255), nullable=False))]
    encrypted_pgbouncer_admin_password: Annotated[str, Field(default="", sa_column=Column(Text, nullable=False))]

    pgbouncer_config: Optional["PgbouncerConfig"] = Relationship(
        back_populates="branch",
        sa_relationship_kwargs={"uselist": False, "cascade": "all, delete-orphan"},
    )

    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS, sa_column=Column(BigInteger))]
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS, sa_column=Column(BigInteger))]  # units of milli vCPU
    memory: Annotated[int, Field(**MEMORY_CONSTRAINTS, sa_column=Column(BigInteger))]
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS, sa_column=Column(BigInteger))]
    storage_size: Annotated[
        int | None, Field(**STORAGE_SIZE_CONSTRAINTS, sa_column=Column(BigInteger, nullable=True))
    ] = None
    enable_file_storage: bool = True
    database_image_tag: str
    status: "BranchServiceStatus" = Field(
        default="UNKNOWN",
        sa_column=Column(String(length=64), nullable=False, server_default="UNKNOWN"),
    )
    status_updated_at: datetime | None = Field(default=None, sa_column=Column(DateTimeTZ(), nullable=True))
    jwt_secret: Annotated[str, Field(default=None, sa_column=Column(Text, nullable=True))]
    anon_key: Annotated[str, Field(default=None, sa_column=Column(Text, nullable=True))]
    service_key: Annotated[str, Field(default=None, sa_column=Column(Text, nullable=True))]
    resize_status: "BranchResizeStatus" = Field(
        default="NONE",
        sa_column=Column(String(length=48), nullable=False),
    )
    resize_statuses: dict[str, dict[str, Any]] = Field(
        default_factory=dict,
        sa_column=Column(JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    )
    resource_usage: dict[str, Any] = Field(
        default_factory=_default_resource_usage_payload,
        sa_column=Column(JSONB, nullable=False, server_default=text("'{}'::jsonb")),
    )
    pitr_enabled: bool = Field(default=False, sa_column=Column(Boolean, nullable=False, server_default=text("false")))

    __table_args__ = (UniqueConstraint("project_id", "name", name="unique_branch_name_per_project"),)

    def provisioned_resources(self) -> "ResourcesDefinition":
        """Return the resource definition for the branch provisioning envelope."""

        return ResourcesDefinition(
            milli_vcpu=self.milli_vcpu,
            ram_bytes=self.memory,
            nvme_bytes=self.database_size,
            iops=self.iops,
            storage_bytes=self.storage_size,
        )

    def set_status(self, status: "BranchServiceStatus", *, force_timestamp: bool = False) -> None:
        parsed_status: BranchServiceStatus
        if isinstance(status, BranchServiceStatus):
            parsed_status = status
        else:
            normalized = str(status)
            member = BranchServiceStatus._value2member_map_.get(normalized)
            parsed_status = member if isinstance(member, BranchServiceStatus) else BranchServiceStatus.UNKNOWN

        if not force_timestamp and getattr(self, "status", None) == parsed_status:
            return
        self.status = parsed_status
        self.status_updated_at = datetime.now(UTC)

    @property
    def database_password(self) -> str:
        if not self.encrypted_database_password or not self.encryption_key:
            raise ValueError("Branch database password is not configured.")
        try:
            return decrypt_with_passphrase(self.encrypted_database_password, self.encryption_key)
        except ValueError:
            plaintext = decrypt_with_base64_key(self.encrypted_database_password, self.encryption_key)
            # Re-encrypt using the new passphrase-based scheme so we eventually migrate all rows.
            self.database_password = plaintext
            return plaintext

    @database_password.setter
    def database_password(self, password: str) -> None:
        encrypted, key = encrypt_with_random_passphrase(password)
        self.encrypted_database_password = encrypted
        self.encryption_key = key

    @property
    def pgbouncer_password(self) -> str:
        if not self.encrypted_pgbouncer_admin_password or not self.encryption_key:
            raise ValueError("PgBouncer admin password is not configured.")
        try:
            return decrypt_with_passphrase(
                self.encrypted_pgbouncer_admin_password,
                self.encryption_key,
            )
        except ValueError:
            plaintext = decrypt_with_base64_key(
                self.encrypted_pgbouncer_admin_password,
                self.encryption_key,
            )
            self.pgbouncer_password = plaintext
            return plaintext

    @pgbouncer_password.setter
    def pgbouncer_password(self, password: str) -> None:
        if not self.encryption_key:
            encrypted, key = encrypt_with_random_passphrase(password)
            self.encrypted_pgbouncer_admin_password = encrypted
            self.encryption_key = key
            return
        self.encrypted_pgbouncer_admin_password = encrypt_with_passphrase(password, self.encryption_key)

    def store_resource_usage(self, usage: "ResourceUsageDefinition") -> None:
        self.resource_usage = usage.model_dump()

    def resource_usage_snapshot(self) -> "ResourceUsageDefinition":
        payload = self.resource_usage or {}
        storage_value = payload.get("storage_bytes")
        return ResourceUsageDefinition(
            milli_vcpu=int(payload.get("milli_vcpu") or 0),
            ram_bytes=int(payload.get("ram_bytes") or 0),
            nvme_bytes=int(payload.get("nvme_bytes") or 0),
            iops=int(payload.get("iops") or 0),
            storage_bytes=None if storage_value is None else int(storage_value),
        )


class BranchApiKey(Model, table=True):
    branch_id: Identifier = Model.foreign_key_field("branch", ondelete="CASCADE")
    branch: Branch = Relationship(back_populates="api_keys")
    name: Annotated[str, Field(sa_type=String(255))]
    role: Annotated[str, Field(sa_type=String(32))]
    api_key: Annotated[str, Field(sa_type=Text)]
    description: Annotated[str | None, Field(default=None, sa_column=Column(Text, nullable=True))] = None

    __table_args__ = (UniqueConstraint("branch_id", "name", name="unique_branch_apikey_name"),)


class PgbouncerConfig(Model, table=True):
    DEFAULT_MAX_CLIENT_CONN: ClassVar[int | None] = PGBOUNCER_DEFAULT_MAX_CLIENT_CONN
    DEFAULT_POOL_SIZE: ClassVar[int] = PGBOUNCER_DEFAULT_POOL_SIZE
    DEFAULT_QUERY_WAIT_TIMEOUT: ClassVar[int | None] = PGBOUNCER_DEFAULT_QUERY_WAIT_TIMEOUT
    DEFAULT_RESERVE_POOL_SIZE: ClassVar[int | None] = PGBOUNCER_DEFAULT_RESERVE_POOL_SIZE
    DEFAULT_SERVER_IDLE_TIMEOUT: ClassVar[int | None] = PGBOUNCER_DEFAULT_SERVER_IDLE_TIMEOUT
    DEFAULT_SERVER_LIFETIME: ClassVar[int | None] = PGBOUNCER_DEFAULT_SERVER_LIFETIME

    branch_id: Identifier = Model.foreign_key_field("branch", nullable=False, unique=True)
    branch: Branch = Relationship(back_populates="pgbouncer_config")

    max_client_conn: Annotated[int | None, Field(default=None, ge=1)]
    default_pool_size: Annotated[int, Field(ge=1)]
    query_wait_timeout: Annotated[int | None, Field(default=None, ge=0)]
    reserve_pool_size: Annotated[int | None, Field(default=None, ge=0)]
    server_idle_timeout: Annotated[int | None, Field(default=None, ge=0)]
    server_lifetime: Annotated[int | None, Field(default=None, ge=0)]


class BranchSourceDeploymentParameters(BaseModel):
    database_password: DBPassword | None = None
    database_size: Annotated[int | None, Field(default=None, **DATABASE_SIZE_CONSTRAINTS)] = None
    storage_size: Annotated[int | None, Field(default=None, **STORAGE_SIZE_CONSTRAINTS)] = None
    milli_vcpu: Annotated[int | None, Field(default=None, **CPU_CONSTRAINTS)] = None
    memory_bytes: Annotated[int | None, Field(default=None, **MEMORY_CONSTRAINTS)] = None
    iops: Annotated[int | None, Field(default=None, **IOPS_CONSTRAINTS)] = None
    enable_file_storage: bool | None = None


class BranchSourceParameters(BaseModel):
    branch_id: Identifier
    config_copy: bool = False
    data_copy: bool = False
    deployment_parameters: BranchSourceDeploymentParameters | None = PydanticField(default=None)


class BranchRestoreParameters(BaseModel):
    backup_id: Identifier
    config_copy: bool = True
    deployment_parameters: BranchSourceDeploymentParameters | None = PydanticField(default=None)


class BranchCreate(BaseModel):
    name: Name
    env_type: str | None = None
    source: BranchSourceParameters | None = None
    deployment: DeploymentParameters | None = None
    restore: BranchRestoreParameters | None = None
    pitr_enabled: bool = False

    @model_validator(mode="after")
    def _validate_source_or_deployment(self) -> "BranchCreate":
        provided = sum(value is not None for value in (self.source, self.deployment, self.restore))
        if provided != 1:
            raise ValueError("Provide exactly one of source, deployment, or restore")
        return self


class BranchUpdate(BaseModel):
    name: Name | None = None


class BranchPasswordReset(BaseModel):
    new_password: DBPassword


class BranchPgbouncerConfigUpdate(BaseModel):
    default_pool_size: Annotated[
        int | None,
        PydanticField(
            ge=PGBOUNCER_DEFAULT_POOL_SIZE,
            description="Number of client connections allowed per database/user pair.",
        ),
    ] = None
    max_client_conn: Annotated[
        int | None,
        PydanticField(ge=PGBOUNCER_DEFAULT_MAX_CLIENT_CONN),
    ] = None
    server_idle_timeout: Annotated[
        int | None,
        PydanticField(ge=PGBOUNCER_DEFAULT_SERVER_IDLE_TIMEOUT),
    ] = None
    server_lifetime: Annotated[
        int | None,
        PydanticField(ge=PGBOUNCER_DEFAULT_SERVER_LIFETIME),
    ] = None
    query_wait_timeout: Annotated[
        int | None,
        PydanticField(ge=PGBOUNCER_DEFAULT_QUERY_WAIT_TIMEOUT),
    ] = None
    reserve_pool_size: Annotated[
        int | None,
        PydanticField(ge=PGBOUNCER_DEFAULT_RESERVE_POOL_SIZE),
    ] = None

    @model_validator(mode="after")
    def ensure_updates(self) -> "BranchPgbouncerConfigUpdate":
        if (
            self.default_pool_size is None
            and self.max_client_conn is None
            and self.server_idle_timeout is None
            and self.server_lifetime is None
            and self.query_wait_timeout is None
            and self.reserve_pool_size is None
        ):
            raise ValueError("Provide at least one PgBouncer parameter to update.")
        return self


class BranchPgbouncerConfigStatus(BaseModel):
    pgbouncer_enabled: bool
    pgbouncer_status: Annotated[str, PydanticField(min_length=1)]
    pool_mode: Annotated[str, PydanticField(min_length=1)]
    max_client_conn: int | None = None
    default_pool_size: int
    server_idle_timeout: int | None = None
    server_lifetime: int | None = None
    query_wait_timeout: int | None = None
    reserve_pool_size: int | None = None


class BranchServiceStatus(str, Enum):
    description: str

    ACTIVE_HEALTHY = "ACTIVE_HEALTHY", "All branch services report healthy and are serving traffic."
    STOPPED = "STOPPED", "Branch is stopped and its services are offline."
    STARTING = "STARTING", "Branch is booting after a stop; services are coming online."
    ACTIVE_UNHEALTHY = "ACTIVE_UNHEALTHY", "At least one branch service is reporting an unhealthy state."
    CREATING = "CREATING", "Branch is being created, cloned, or restarted and resources are provisioning."
    DELETING = "DELETING", "Branch resources are being torn down as part of a delete operation."
    UPDATING = "UPDATING", "Branch is undergoing a resource update such as CPU, memory, storage, or engine upgrades."
    RESTARTING = "RESTARTING", "Branch is restarting services without additional side effects (placeholder state)."
    STOPPING = "STOPPING", "Branch is in the process of stopping; services are shutting down."
    PAUSING = "PAUSING", "Branch is being hibernated and workloads are suspending."
    PAUSED = "PAUSED", "Branch is hibernated and workloads are suspended."
    RESUMING = "RESUMING", "Branch is resuming from hibernation and services are being restored."
    UNKNOWN = "UNKNOWN", "Branch status is currently indeterminate or unavailable."
    ERROR = "ERROR", "An internal error prevented the branch state from being determined."
    RESIZING = "RESIZING", "A resize operation is currently in progress."

    def __new__(cls, value: str, description: str):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        return obj


class DatabaseInformation(BaseModel):
    host: str
    port: int
    username: str
    name: str
    encrypted_connection_string: str
    service_endpoint_uri: str
    monitoring_endpoint_uri: str | None
    version: str
    has_replicas: bool


class ResourcesDefinition(BaseModel):
    milli_vcpu: Annotated[
        int,
        PydanticField(
            gt=0,
            description="Number of milli vCPUs provisioned (matches Branch.milli_vcpu constraints).",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            gt=0,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            gt=0,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            gt=0,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int | None,
        PydanticField(
            gt=0,
            description="Storage capacity in bytes to be used for Storage API (mirrors Branch.storage_size).",
        ),
    ] = None


class ResourceUsageDefinition(BaseModel):
    milli_vcpu: Annotated[
        int,
        PydanticField(
            ge=0,
            le=2**31 - 1,
            description="Measured vCPU consumption for the branch.",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            ge=0,
            description="Measured RAM usage in bytes.",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            ge=0,
            description="Measured NVMe usage in bytes.",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            ge=0,
            le=2**31 - 1,
            description="Measured IOPS consumption.",
        ),
    ]
    storage_bytes: Annotated[
        int | None,
        PydanticField(
            ge=0,
            description="Measured storage usage in bytes, if available.",
        ),
    ] = None


class BranchApiKeys(BaseModel):
    anon: str | None
    service_role: str | None


ApiKeyRole = Literal["anon", "service_role"]


class ApiKeyCreate(BaseModel):
    name: Name
    role: ApiKeyRole
    description: str | None = None


class ApiKeyDetails(BaseModel):
    name: str
    role: ApiKeyRole
    api_key: str
    id: str
    hash: str
    prefix: str
    description: str

    @classmethod
    def from_entry(cls, entry: "BranchApiKey") -> "ApiKeyDetails":
        key = entry.api_key
        description = entry.description or f"{entry.role} API key"
        return cls(
            name=entry.name,
            role=cast("ApiKeyRole", entry.role),
            api_key=key,
            id=str(entry.id),
            hash=hashlib.sha256(key.encode()).hexdigest(),
            prefix=key[:5],
            description=description,
        )


class BranchStatus(BaseModel):
    database: BranchServiceStatus
    storage: BranchServiceStatus
    meta: BranchServiceStatus
    rest: BranchServiceStatus


BranchResizeStatus = Literal[
    "NONE",
    "PENDING",
    "RESIZING",
    "FILESYSTEM_RESIZE_PENDING",
    "COMPLETED",
    "FAILED",
]

CapaResizeKey = Literal["database_size", "storage_size", "milli_vcpu", "memory_bytes", "iops"]

BranchResizeService = Literal[
    "database_disk_resize",
    "storage_api_disk_resize",
    "database_cpu_resize",
    "database_memory_resize",
    "database_iops_resize",
]

RESIZE_STATUS_PRIORITY: dict[BranchResizeStatus, int] = {
    "NONE": 0,
    "PENDING": 1,
    "RESIZING": 2,
    "FILESYSTEM_RESIZE_PENDING": 3,
    "COMPLETED": 4,
    "FAILED": 5,
}


class BranchResizeStatusEntry(BaseModel):
    """Single service's resize state and the timestamp when it was observed."""

    model_config = ConfigDict(extra="allow")

    status: BranchResizeStatus
    timestamp: str

    def timestamp_as_datetime(self) -> datetime:
        value = self.timestamp
        if not value:
            return datetime.min.replace(tzinfo=UTC)
        try:
            normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return datetime.min.replace(tzinfo=UTC)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)


def should_transition_resize_status(current: BranchResizeStatus | None, proposed: BranchResizeStatus) -> bool:
    """Return True when the proposed status should replace the current one."""

    if proposed not in RESIZE_STATUS_PRIORITY:
        return False
    if current == proposed:
        return False
    if proposed == "FAILED":
        return True
    if current is None:
        return True
    return RESIZE_STATUS_PRIORITY[proposed] >= RESIZE_STATUS_PRIORITY[current]


def aggregate_resize_statuses(
    statuses: Mapping[str, BranchResizeStatusEntry | Mapping[str, Any]] | None,
) -> BranchResizeStatus:
    """
    Aggregate per-service resize statuses into a single branch-level status.
    """
    highest: BranchResizeStatus = "NONE"
    if not statuses:
        return highest

    highest_timestamp = datetime.min.replace(tzinfo=UTC)
    for entry in statuses.values():
        if isinstance(entry, BranchResizeStatusEntry):
            snapshot = entry
        else:
            try:
                snapshot = BranchResizeStatusEntry.model_validate(entry)
            except ValidationError:
                continue

        status = snapshot.status
        if status not in RESIZE_STATUS_PRIORITY:
            continue
        timestamp = snapshot.timestamp_as_datetime()
        if timestamp > highest_timestamp or (
            timestamp == highest_timestamp and should_transition_resize_status(highest, status)
        ):
            highest = status
            highest_timestamp = timestamp
    return highest


class BranchPublic(BaseModel):
    id: Identifier
    name: Name
    env_type: str | None
    project_id: Identifier
    organization_id: Identifier
    database: DatabaseInformation
    max_resources: ResourcesDefinition
    assigned_labels: list[str]
    used_resources: ResourceUsageDefinition
    api_keys: BranchApiKeys
    status: BranchServiceStatus
    pitr_enabled: bool
    created_at: datetime
    created_by: str
    updated_at: datetime | None = None
    updated_by: str | None = None


class BranchStatusPublic(BaseModel):
    resize_status: BranchResizeStatus
    resize_statuses: dict[str, BranchResizeStatusEntry]
    service_status: BranchStatus


class BranchDetailResources(BaseModel):
    milli_vcpu: Annotated[
        int,
        PydanticField(
            **CPU_CONSTRAINTS,
            description="Number of milli vCPUs provisioned (matches Branch.milli_vcpu constraints).",
        ),
    ]
    ram_bytes: Annotated[
        int,
        PydanticField(
            **MEMORY_CONSTRAINTS,
            description="Guest memory expressed in bytes (mirrors Branch.memory).",
        ),
    ]
    nvme_bytes: Annotated[
        int,
        PydanticField(
            **DATABASE_SIZE_CONSTRAINTS,
            description="Provisioned NVMe volume capacity in bytes (derived from Branch.database_size).",
        ),
    ]
    iops: Annotated[
        int,
        PydanticField(
            **IOPS_CONSTRAINTS,
            description="Configured storage IOPS budget (matches Branch.iops constraints).",
        ),
    ]
    storage_bytes: Annotated[
        int,
        PydanticField(
            **STORAGE_SIZE_CONSTRAINTS,
            description="Database storage capacity in bytes (mirrors Branch.database_size).",
        ),
    ]

from typing import Annotated, Literal

from pydantic import BaseModel, Field

from .._util import (
    CPU_CONSTRAINTS,
    DATABASE_SIZE_CONSTRAINTS,
    IOPS_CONSTRAINTS,
    MEMORY_CONSTRAINTS,
    STORAGE_SIZE_CONSTRAINTS,
    DBPassword,
    StatusType,
)

class DeploymentParameters(BaseModel):
    database_password: DBPassword
    database_size: Annotated[int, Field(**DATABASE_SIZE_CONSTRAINTS)]
    storage_size: Annotated[int, Field(**STORAGE_SIZE_CONSTRAINTS)]
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS)]  # units of milli vCPU
    memory_bytes: Annotated[int, Field(**MEMORY_CONSTRAINTS)]
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS)]
    database_image_tag: Literal["15.1.0.147"]
    enable_file_storage: bool = True


class DeploymentStatus(BaseModel):
    status: StatusType

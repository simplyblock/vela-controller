from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

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
    storage_size: Annotated[int | None, Field(**STORAGE_SIZE_CONSTRAINTS)] = None
    milli_vcpu: Annotated[int, Field(**CPU_CONSTRAINTS)]  # units of milli vCPU
    memory_bytes: Annotated[int, Field(**MEMORY_CONSTRAINTS)]
    iops: Annotated[int, Field(**IOPS_CONSTRAINTS)]
    database_image_tag: Literal["15.1.0.147"]
    enable_file_storage: bool = True

    @model_validator(mode="after")
    def ensure_storage_requirements(self) -> "DeploymentParameters":
        if self.enable_file_storage and self.storage_size is None:
            raise ValueError("storage_size is required when file storage is enabled")
        return self


class DeploymentStatus(BaseModel):
    status: StatusType

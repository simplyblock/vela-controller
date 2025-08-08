from typing import Optional, List
from pydantic import BaseModel

class VelaCreateRequest(BaseModel):
    namespace: str
    dbuser: str
    dbname: str
    dbpassword: str
    vcpu: int
    ram: int  # MB
    db_storage: int  # GB
    iops: int

class ErrorResponse(BaseModel):
    error: str
    reason: Optional[str] = None

class DeploymentCreateResponse(BaseModel):
    deployment_id: str

class DeploymentItem(BaseModel):
    id: str
    namespace: str
    release_name: str
    db_user: str
    db_name: str
    status: str
    created_at: str
    updated_at: Optional[str] = None

class DeploymentStatusResponse(BaseModel):
    deployment_id: str
    namespace: str
    status: str
    pods: list
    message: str

class DeleteDeploymentResponse(BaseModel):
    status: str
    deployment_id: str
    helm_output: str

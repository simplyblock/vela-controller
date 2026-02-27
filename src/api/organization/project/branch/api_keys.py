import hashlib
from datetime import UTC, datetime
from typing import Literal, cast

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, field_validator
from sqlalchemy.exc import IntegrityError

from ....._util import Name
from ....._util.crypto import generate_keys
from .....models.branch import BranchApiKey
from ...._util import Conflict, Forbidden, NotFound, Unauthenticated
from ....dependencies import ApiKeyDep, BranchDep, OrganizationDep, ProjectDep, SessionDep

api = APIRouter()

ApiKeyRole = Literal["anon", "service_role"]


class ApiKeyDetails(BaseModel):
    name: str
    role: ApiKeyRole
    api_key: str
    id: str
    hash: str
    prefix: str
    description: str
    expiry_timestamp: datetime

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
            expiry_timestamp=entry.expiry_timestamp,
        )


@api.get(
    "/",
    name="organizations:projects:branch:apikeys",
    response_model=list[ApiKeyDetails],
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_(
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
) -> list[ApiKeyDetails]:
    key_entries = await branch.awaitable_attrs.api_keys
    key_entries = sorted(key_entries, key=lambda entry: str(entry.id))

    return [ApiKeyDetails.from_entry(entry) for entry in key_entries]


class ApiKeyCreate(BaseModel):
    name: Name
    role: ApiKeyRole
    description: str | None = None
    expiry_timestamp: datetime

    @field_validator("expiry_timestamp")
    @classmethod
    def validate_expiry_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("expiry_timestamp must include a timezone.")
        expiry = value.astimezone(UTC)
        if expiry <= datetime.now(UTC):
            raise ValueError("expiry_timestamp must be in the future.")
        return expiry


@api.post(
    "/",
    name="organizations:projects:branch:apikeys:create",
    response_model=ApiKeyDetails,
    status_code=201,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound, 409: Conflict},
)
async def create(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    branch: BranchDep,
    parameters: ApiKeyCreate,
) -> ApiKeyDetails:
    if not branch.jwt_secret:
        raise HTTPException(status_code=400, detail="Branch JWT secret is not configured.")

    anon_key, service_key = generate_keys(str(branch.id), branch.jwt_secret, expires_at=parameters.expiry_timestamp)
    api_key = anon_key if parameters.role == "anon" else service_key
    entry = BranchApiKey(
        branch_id=branch.id,
        name=parameters.name,
        role=parameters.role,
        api_key=api_key,
        description=parameters.description,
        expiry_timestamp=parameters.expiry_timestamp,
    )
    session.add(entry)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        error = str(exc)
        if "unique_branch_apikey_name" in error:
            raise HTTPException(status_code=409, detail=f"API key name {parameters.name} already exists.") from exc
        raise
    await session.refresh(entry)

    return ApiKeyDetails.from_entry(entry)


@api.delete(
    "/{api_key_id}",
    name="organizations:projects:branch:apikeys:delete",
    status_code=204,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete(
    session: SessionDep,
    _organization: OrganizationDep,
    _project: ProjectDep,
    _branch: BranchDep,
    api_key: ApiKeyDep,
) -> Response:
    await session.delete(api_key)
    await session.commit()
    return Response(status_code=204)

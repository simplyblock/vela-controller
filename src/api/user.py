import secrets
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr

from ._util import NotFound, Unauthenticated
from .auth import authenticated_user
from .keycloak import admin as keycloak_admin
from .models.user import UserParameters, UserPublic

api = APIRouter(dependencies=[Depends(authenticated_user)])


@api.get(
    "/{user_ref}/",
    responses={401: Unauthenticated, 404: NotFound},
)
async def get(user_ref: UUID | EmailStr) -> UserPublic:
    if isinstance(user_ref, EmailStr):
        user_id = await keycloak_admin.a_get_user_id(str(user_ref))
    else:
        user_id = str(user_ref)

    user = await keycloak_admin.a_get_user(user_id)
    return UserPublic(
        id=user["id"],
        email=user["email"],
        first_name=user["firstName"],
        last_name=user["lastName"],
        email_verified=user["emailVerified"],
    )


class UserCreationResult(BaseModel):
    id: UUID
    password: str


@api.post(
    "/",
    status_code=201,
    responses={401: Unauthenticated},
)
async def add(parameters: UserParameters) -> tuple[UserCreationResult, int]:
    password = secrets.token_hex(16)
    user_id = await keycloak_admin.a_create_user(
        {
            "email": parameters.email,
            "enabled": True,
            "firstName": parameters.first_name,
            "lastName": parameters.last_name,
            "credentials": [
                {
                    "value": password,
                    "type": "password",
                    "temporary": True,
                }
            ],
        }
    )
    await keycloak_admin.a_send_verify_email(user_id)
    return UserCreationResult(
        id=UUID(user_id),
        password=password,
    ), 201

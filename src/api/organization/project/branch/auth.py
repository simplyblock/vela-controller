from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from keycloak import KeycloakAdmin
from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    HttpUrl,
    SerializationInfo,
    TypeAdapter,
    model_serializer,
    model_validator,
)
from pydantic.alias_generators import to_camel

from ...._util import Forbidden, NotFound, Port, Unauthenticated
from ....keycloak import realm_admin
from ....models.branch import BranchDep

api = APIRouter()


def _branch_realm(branch: BranchDep):
    return realm_admin(str(branch.id))


BranchRealmDep = Annotated[KeycloakAdmin, Depends(_branch_realm)]


class IdentityProviderParameters(BaseModel):
    name: str
    provider_id: Literal["oidc"]
    client_id: str
    client_secret: str
    authorization_url: HttpUrl
    token_url: HttpUrl
    issuer: HttpUrl | None
    user_info_url: HttpUrl | None


class IdentityProvider(BaseModel):
    name: str = Field(alias="alias")
    provider_id: Literal["oidc"] = Field(alias="providerId")
    client_id: str = Field(alias="config.clientId")
    authorization_url: HttpUrl = Field(alias="config.authorizationUrl")
    token_url: HttpUrl = Field(alias="config.tokenUrl")
    issuer: HttpUrl | None = Field(alias="config.issuer")
    user_info_url: HttpUrl | None = Field(alias="config.userInfoUrl")

    @model_validator(mode="before")
    @classmethod
    def flatten_config(cls, data: Any) -> Any:
        if isinstance(data, dict) and "config" in data:
            # Flatten the config dictionary
            config = data.pop("config")
            for key, value in config.items():
                data[f"config.{key}"] = value
        return data


@api.get(
    "/oauth/",
    name="organizations:projects:branch:auth:oauth:list",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def list_oauth(realm: BranchRealmDep) -> list[IdentityProvider]:
    return TypeAdapter(list[IdentityProvider]).validate_python(await realm.a_get_idps())


@api.post(
    "/oauth/",
    name="organizations:projects:branch:auth:oauth:create",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def create_oauth(realm: BranchRealmDep, parameters: IdentityProviderParameters):
    return await realm.a_create_idp(
        {
            "alias": parameters.name,
            "providerId": parameters.provider_id,
            "config": {
                "clientId": parameters.client_id,
                "clientSecret": parameters.client_secret,
                "authorizationUrl": parameters.authorization_url,
                "tokenUrl": parameters.token_url,
                "issuer": parameters.issuer,
                "user_info_url": parameters.user_info_url,
            },
        }
    )


@api.get(
    "/oauth/{oauth_name}/",
    name="organizations:projects:branch:auth:oauth:get",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def get_oauth(realm: BranchRealmDep, oauth_name: str) -> IdentityProvider:
    return IdentityProvider.model_validate(await realm.a_get_idp(oauth_name))


@api.delete(
    "/oauth/{oauth_name}/",
    name="organizations:projects:branch:auth:oauth:delete",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def delete_oauth(realm: BranchRealmDep, oauth_name: str):
    return await realm.a_delete_idp(oauth_name)


class SMTPConfig(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    host: str
    port: Port
    from_: EmailStr = Field(alias="from")
    from_display_name: str
    reply_to: EmailStr
    reply_to_display_name: str
    user: str
    password: str
    tls: Literal["starttls", "ssl"] | None

    @model_validator(mode="before")
    @classmethod
    def from_keycloak(cls, data: dict) -> dict:
        if isinstance(data, dict) and ("a" in data or "b" in data):
            starttls = data.pop("starttls", False)
            ssl = data.pop("ssl", False)

            if starttls and ssl:
                raise ValueError("Both 'starttls' and 'ssl' cannot be true")

            data["tls"] = "starttls" if starttls else ("ssl" if ssl else None)

        return data

    @model_serializer(mode="wrap")
    def serialize_with_context(self, serializer, info: SerializationInfo):
        data = serializer(self)

        # Check for explicit context flag
        if info.context and info.context.get("keycloak"):
            tls = data.pop("tls", None)
            data["starttls"] = tls == "starttls"
            data["ssl"] = tls == "ssl"

        return data


@api.get(
    "/smtp/",
    name="organizations:projects:branch:auth:smtp:get",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def get_smtp(realm: BranchRealmDep) -> SMTPConfig | None:
    smtp_config = (await realm.a_get_realm(realm.connection.realm_name))["smtpServer"]
    return SMTPConfig.model_validate(smtp_config)


@api.get(
    "/smtp/",
    name="organizations:projects:branch:auth:smtp:update",
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def update_smtp(realm: BranchRealmDep, parameters: SMTPConfig):
    return realm.a_update_realm(
        realm.connection.realm_name, {"smtpServer": parameters.model_dump(by_alias=True, context={"keycloak": True})}
    )


# TODO 2FA

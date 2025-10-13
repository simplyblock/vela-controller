from typing import Annotated

from pydantic import Field, HttpUrl, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)

    deployment_namespace_prefix: Annotated[
        str,
        StringConstraints(
            min_length=1,
            max_length=36,
            pattern=r"^[a-z][a-z0-9._-]*[a-z0-9]$",
        ),
    ] = "vela"
    logflare_private_access_token: Annotated[
        str,
        Field(..., description="Private access token for authenticating with the Logflare API."),
    ]
    logflare_url: Annotated[
        HttpUrl,
        Field(..., description="Base URL of the Logflare API (e.g. http://localhost:4000"),
    ] = "http://localhost:4000"
    deployment_release_name: str = "supabase"
    deployment_host: str = "localhost"
    pgmeta_crypto_key: str
    cloudflare_api_token: str
    cloudflare_zone_id: str
    cloudflare_branch_ref_cname: str
    cloudflare_domain_suffix: str
    gateway_name: str = "vela-public-gateway"
    gateway_namespace: str = "kong-system"


settings = Settings()  # type: ignore[call-arg]

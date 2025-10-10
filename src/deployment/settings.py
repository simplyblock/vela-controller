from typing import Annotated

from pydantic import StringConstraints
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
    deployment_release_name: str = "supabase"
    deployment_host: str = "localhost"
    pgmeta_crypto_key: str
    cloudflare_api_token: str
    cloudflare_zone_id: str
    cloudflare_branch_ref_cname: str
    cloudflare_domain_suffix: str
    gateway_name: str = "vela-public-gateway"
    gateway_namespace: str = "kong-system"
    logflare_public_access_token: str
    grafana_url: str
    grafana_security_admin_user: str
    grafana_security_admin_password: str


settings = Settings()  # type: ignore[call-arg]

from functools import lru_cache
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
        Field(default="", description="Private access token for authenticating with the Logflare API."),
    ]

    logflare_public_access_token: Annotated[
        str,
        Field(default="", description="Public access token for authenticating with the Logflare API."),
    ]

    logflare_url: Annotated[
        HttpUrl,
        Field(default="http://localhost:4000", description="Base URL of the Logflare API (e.g. http://localhost:4000)"),
    ]
    deployment_release_name: str = "vela"
    server_root_url: str = "http://localhost:8000"
    deployment_service_port: int = 443
    enable_db_external_ipv6_loadbalancer: bool = True  # Expose Vela Postgres via external IPv6 LB when true
    enable_autoscaler_vm_extra_network: bool = True
    pgmeta_crypto_key: str
    cloudflare_api_token: str
    cloudflare_zone_id: str
    cloudflare_branch_ref_cname: str
    cloudflare_domain_suffix: str
    gateway_name: str = "vela-public-gateway"
    gateway_namespace: str = "kong-system"
    grafana_url: str = "http://localhost:3000"
    grafana_security_admin_user: str = "admin"
    grafana_security_admin_password: str = "password"


@lru_cache
def get_settings():
    return Settings()  # type: ignore[call-arg]

from functools import lru_cache
from typing import Annotated

from pydantic import BaseModel, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict


class CloudflareSettings(BaseModel):
    api_token: str
    zone_id: str
    branch_ref_cname: str
    branch_db_ref_cname: str
    domain_suffix: str


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="vela_",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

    deployment_namespace_prefix: Annotated[
        str,
        StringConstraints(
            min_length=1,
            max_length=36,
            pattern=r"^[a-z][a-z0-9._-]*[a-z0-9]$",
        ),
    ] = "vela"
    deployment_release_name: str = "vela"
    server_root_url: str = "http://localhost:8000"
    deployment_service_port: int = 443
    enable_db_external_ipv6_loadbalancer: bool = True  # Expose Vela Postgres via external IPv6 LB when true
    pgmeta_crypto_key: str
    cloudflare: CloudflareSettings
    gateway_name: str = "vela-public-gateway"
    gateway_namespace: str = "kong-system"
    grafana_url: str = "http://localhost:3000"
    grafana_security_admin_user: str = "admin"
    grafana_security_admin_password: str = "password"


@lru_cache
def get_settings():
    return Settings()  # type: ignore[call-arg]

from functools import lru_cache

from pydantic import HttpUrl, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)

    jwt_secret: str
    postgres_url: PostgresDsn
    root_path: str = ""
    cors_origins: list[str] = []
    jwt_algorithms: list[str] = ["HS256", "HS512", "RS256"]
    pgmeta_crypto_key: str
    keycloak_url: HttpUrl
    keycloak_realm: str = "vela"
    keycloak_admin_name: str
    keycloak_admin_secret: str
    system_limit_millis_vcpu: int
    system_limit_ram: int
    system_limit_iops: int
    system_limit_storage_size: int
    system_limit_database_size: int


@lru_cache
def get_settings():
    return Settings()  # type: ignore[call-arg]

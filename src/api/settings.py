from datetime import timedelta
from functools import lru_cache
from typing import Annotated, Literal

from pydantic import BeforeValidator, HttpUrl, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict

from .._util import permissive_numeric_timedelta


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
    log_level: Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] = "INFO"
    resource_monitor_interval: Annotated[timedelta, BeforeValidator(permissive_numeric_timedelta)] = timedelta(
        seconds=60
    )


@lru_cache
def get_settings():
    return Settings()  # type: ignore[call-arg]

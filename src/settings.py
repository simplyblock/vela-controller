from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)

    jwt_secret: str
    postgres_url: PostgresDsn
    root_path: str = ""
    cors_origins: list[str] = []
    jwt_algorithms: list[str] = ["HS256", "HS512", "RS256"]
    pgmeta_crypto_key: str
    deployment_namespace_prefix: str = "vela"
    deployment_host: str = "_"


settings = Settings()  # type: ignore[call-arg]

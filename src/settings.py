from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='vela_', case_sensitive=False)

    postgres_url: PostgresDsn


settings = Settings()  # type: ignore[call-arg]

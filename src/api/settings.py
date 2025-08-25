from pydantic import PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix='vela_', case_sensitive=False)

    jwt_secret: str
    postgres_url: PostgresDsn
    root_path: str = ''


settings = Settings()  # type: ignore[call-arg]

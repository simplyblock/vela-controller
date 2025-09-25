from pydantic import AnyUrl, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)

    jwt_secret: str
    postgres_url: PostgresDsn
    root_path: str = ""
    cors_origins: list[str] = []
    jwt_algorithms: list[str] = ["HS256", "HS512", "RS256"]
    pgmeta_crypto_key: str
    sentry_dsn: AnyUrl | None = None
    sentry_traces_sample_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    sentry_profiles_sample_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    sentry_environment: str | None = None


settings = Settings()  # type: ignore[call-arg]

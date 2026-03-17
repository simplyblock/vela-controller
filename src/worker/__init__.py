from celery import Celery
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)
    broker_url: str
    result_backend: str


_settings = Settings()  # type: ignore[call-arg]

app = Celery("vela", broker=_settings.broker_url, backend=_settings.result_backend)

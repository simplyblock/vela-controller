from celery import Celery
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)
    broker_url: str
    result_backend: str


_settings = Settings()  # type: ignore[call-arg]

app = Celery("vela", broker=_settings.broker_url, backend=_settings.result_backend)

# Persist task name, args, and kwargs in celery_taskmeta so AsyncResult can
# reconstruct full task details without a custom model.
app.conf.result_extended = True

# Register tasks — must be imported after `app` is defined.
from ..deployment import resize as _  # noqa: E402, F401

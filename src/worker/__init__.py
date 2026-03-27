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

# Chord callback always fires even when individual sub-tasks fail.
app.conf.task_chord_propagates = False

# Register tasks — must be imported after `app` is defined.
from ..api.organization.project.branch import resize_tasks as _api_resize_tasks  # noqa: E402, F401
from ..deployment import resize as _deployment_resize  # noqa: E402, F401

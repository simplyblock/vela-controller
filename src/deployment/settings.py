from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="vela_", case_sensitive=False)

    deployment_namespace_prefix: str = "vela"
    deployment_host: str = "localhost"
    cloudflare_api_token: str = ""
    cloudflare_zone_id: str = ""
    cloudflare_dns_target: str = ""
    cloudflare_domain_suffix: str = ""


settings = Settings()  # type: ignore[call-arg]

from keycloak import KeycloakAdmin

from .settings import settings

admin = KeycloakAdmin(
    str(settings.keycloak_url),
    client_id=settings.keycloak_client_id,
    client_secret_key=settings.keycloak_client_secret,
    realm_name=settings.keycloak_realm,
    verify=True,
)

from keycloak import KeycloakAdmin

from .settings import settings


def realm_admin(realm_name: str) -> KeycloakAdmin:
    return KeycloakAdmin(
        server_url=str(settings.keycloak_url),
        username=settings.keycloak_admin_name,
        password=settings.keycloak_admin_secret,
        realm_name=realm_name,
        user_realm_name="master",
    )

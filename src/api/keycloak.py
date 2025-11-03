from keycloak import KeycloakAdmin

from .settings import get_settings


def realm_admin(realm_name: str) -> KeycloakAdmin:
    return KeycloakAdmin(
        server_url=str(get_settings().keycloak_url),
        username=get_settings().keycloak_admin_name,
        password=get_settings().keycloak_admin_secret,
        realm_name=realm_name,
        user_realm_name="master",
    )

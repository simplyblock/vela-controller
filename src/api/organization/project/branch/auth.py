from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from keycloak import KeycloakAdmin
from keycloak.exceptions import KeycloakError

from ....keycloak import realm_admin
from ....models.branch import BranchDep

api = APIRouter()


def _branch_realm(branch: BranchDep):
    return realm_admin(str(branch.id))


BranchRealmDep = Annotated[KeycloakAdmin, Depends(_branch_realm)]


@api.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE"],
    include_in_schema=False,  # Proxies Keycloak, we do not want this endpoint itself present
)
async def proxy_keycloak_admin(
    realm: BranchRealmDep,
    path: str,
    request: Request,
):
    """Proxy requests to Keycloak admin API for a specific realm"""
    # TODO Handle authorization

    keycloak_root_path = "/auth"
    prefix = f"{keycloak_root_path}/admin/realms/{realm.connection.realm_name}"
    api_path = f"{prefix}/{path}" if path != "" else prefix

    try:
        if request.method == "GET":
            response = await realm.connection.a_raw_get(api_path)  # type: ignore[arg-type]
        elif request.method == "POST":
            response = await realm.connection.a_raw_post(api_path, data=await request.body())  # type: ignore[arg-type]
        elif request.method == "PUT":
            response = await realm.connection.a_raw_put(api_path, data=await request.body())  # type: ignore[arg-type]
        elif request.method == "DELETE":
            response = await realm.connection.a_raw_delete(api_path)  # type: ignore[arg-type]
        else:
            raise HTTPException(status_code=405, detail="Method not allowed")

        print(response.content)
        return JSONResponse(
            content=response.json() if response.content else {},
            status_code=response.status_code,
            headers=dict(response.headers),
        )

    except KeycloakError as e:
        raise HTTPException(status_code=502, detail=f"Keycloak error: {str(e)}") from e

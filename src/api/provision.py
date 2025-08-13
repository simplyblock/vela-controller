from fastapi import APIRouter, Request, Response

from ..deployment import (
    Deployment,
    DeploymentParameters,
    DeploymentStatus,
    create_vela_config,
)
from ..deployment import (
    delete_deployment as deployment_delete, DeleteDeploymentRequest,
)
from ..deployment import (
    get_deployment_status as deployment_status,
)
from ._util import Forbidden, NotFound, Unauthenticated
from .db import SessionDep

api = APIRouter()


@api.post(
        '/', name='provision:create', status_code=201,
        responses={
            201: {
                'content': None,
                'headers': {
                    'Location': {
                        'description': 'URL of the created deployment',
                        'schema': {'type': 'string'},
                    },
                },
                'links': {
                    'detail': {
                        'operationId': 'provision:detail',
                        'parameters': {'namespace': '$response.header.Location#regex:/provision/(.+)/'},
                    },
                    'delete': {
                        'operationId': 'provision:delete',
                        'parameters': {'namespace': '$response.header.Location#regex:/provision/(.+)/'},
                    },
                },
            },
            401: Unauthenticated,
            403: Forbidden,
            404: NotFound,
        },
)
async def create(_session: SessionDep, request: Request, parameters: DeploymentParameters) -> Response:
    """Create a new deployment.

    Request body is `DeploymentParameters`.
    On success returns 201 with `Location` header to GET the deployment status.
    """
    deployment = create_vela_config(parameters)
    entity_url = request.app.url_path_for('provision:detail', namespace=deployment.namespace)
    return Response(status_code=201, headers={'Location': entity_url})


instance_api = APIRouter(prefix='/{namespace}')


@instance_api.get(
        '/', name='provision:detail', response_model=DeploymentStatus,
        responses={
            200: {'model': DeploymentStatus},
            401: Unauthenticated, 403: Forbidden, 404: NotFound,
        },
)
async def detail(namespace: str) -> DeploymentStatus:
    """Get deployment status for the given `namespace`.

    Returns `DeploymentStatus` including current status and pod list.
    """
    # Only namespace is needed to compute status; construct minimal Deployment
    dep = Deployment(namespace=namespace, release_name=f'supabase-{namespace}', database_user='', database_name='')
    return deployment_status(dep)


@instance_api.delete(
        '/', name='provision:delete', status_code=204,
        responses={
            204: {'content': None},
            401: Unauthenticated, 403: Forbidden, 404: NotFound,
        },
)
async def delete(_session: SessionDep, namespace: str):
    """Delete the deployment in the given `namespace`.

    Returns 204 on success.
    """
    dep = DeleteDeploymentRequest(namespace=namespace)
    return deployment_delete(dep)


api.include_router(instance_api)

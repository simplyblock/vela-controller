from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ._util import Forbidden, NotFound, Unauthenticated

# We load kube config lazily per request to reuse the behavior outside clusters
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException


api = APIRouter()


class KubeVirtActionResponse(BaseModel):
    namespace: str
    name: str
    action: Literal['pause', 'unpause']
    status: Literal['ok', 'error']
    detail: str | None = None


def _ensure_kubeconfig():
    try:
        config.load_incluster_config()
        return
    except config.config_exception.ConfigException:
        try:
            config.load_kube_config()
            return
        except config.config_exception.ConfigException as e:
            raise HTTPException(status_code=503, detail="Kubernetes client not configured. Mount kubeconfig or run in-cluster.") from e


def _call_kubevirt_subresource(namespace: str, name: str, action: Literal['pause', 'unpause']):
    _ensure_kubeconfig()
    api_client = client.ApiClient()
    path = f"/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachineinstances/{name}/{action}"
    # KubeVirt subresources accept POST with empty body
    return api_client.call_api(
        path,
        'POST',
        response_type=(object),
        auth_settings=['BearerToken'],
        body={},
        _check_type=False,
        _preload_content=False,
    )


@api.post(
    '/vmis/{namespace}/{name}/pause',
    name='kubevirt:vmis:pause',
    response_model=KubeVirtActionResponse,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def pause_vmi(namespace: str, name: str) -> KubeVirtActionResponse:
    try:
        _call_kubevirt_subresource(namespace, name, 'pause')
        return KubeVirtActionResponse(namespace=namespace, name=name, action='pause', status='ok')
    except ApiException as e:
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e


@api.post(
    '/vmis/{namespace}/{name}/unpause',
    name='kubevirt:vmis:unpause',
    response_model=KubeVirtActionResponse,
    responses={401: Unauthenticated, 403: Forbidden, 404: NotFound},
)
async def unpause_vmi(namespace: str, name: str) -> KubeVirtActionResponse:
    try:
        _call_kubevirt_subresource(namespace, name, 'unpause')
        return KubeVirtActionResponse(namespace=namespace, name=name, action='unpause', status='ok')
    except ApiException as e:
        status = 404 if e.status == 404 else 400
        raise HTTPException(status_code=status, detail=e.body or str(e)) from e

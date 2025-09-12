from typing import Literal

from fastapi import HTTPException
from kubernetes import client, config


def _ensure_kubeconfig():
    try:
        config.load_incluster_config()
    except config.config_exception.ConfigException:
        try:
            config.load_kube_config()
        except config.config_exception.ConfigException as e:
            raise HTTPException(
                status_code=503,
                detail="Kubernetes client not configured. Mount kubeconfig or run in-cluster.",
            ) from e


def _call_kubevirt_subresource(namespace: str, name: str, action: Literal['pause', 'resume']):
    _ensure_kubeconfig()
    api_client = client.ApiClient()
    path = f"/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachineinstances/{name}/{action}"
    # KubeVirt subresources accept POST with empty body
    return api_client.call_api(
        path,
        'POST',
        response_type=None,
        auth_settings=['BearerToken'],
        body={},
        _preload_content=False,
    )

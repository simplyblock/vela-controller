from typing import Literal

from fastapi import HTTPException
from kubernetes_asyncio import client, config

SubresourceAction = Literal["pause", "resume", "start", "stop"]

_SUBRESOURCE_ACTION_MAP: dict[SubresourceAction, str] = {
    "pause": "pause",
    "resume": "unpause",
    "start": "start",
    "stop": "stop",
}


async def _ensure_kubeconfig() -> None:
    try:
        config.load_incluster_config()
    except config.config_exception.ConfigException:
        try:
            await config.load_kube_config()
        except config.config_exception.ConfigException as e:
            raise HTTPException(
                status_code=503,
                detail="Kubernetes client not configured. Mount kubeconfig or run in-cluster.",
            ) from e


async def call_kubevirt_subresource(namespace: str, name: str, action: SubresourceAction):
    await _ensure_kubeconfig()
    kubevirt_action = _SUBRESOURCE_ACTION_MAP[action]
    path = f"/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachineinstances/{name}/{kubevirt_action}"
    async with client.ApiClient() as api_client:
        return await api_client.call_api(
            path,
            "POST",
            response_type=None,
            auth_settings=["BearerToken"],
            body={},
            _preload_content=False,
        )

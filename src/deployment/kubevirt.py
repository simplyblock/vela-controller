from typing import Any, Literal

from fastapi import HTTPException
from kubernetes_asyncio import client, config


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


async def call_kubevirt_subresource(namespace: str, name: str, action: Literal["pause", "resume", "migrate"]):
    await _ensure_kubeconfig()
    path = f"/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachineinstances/{name}/{action}"
    async with client.ApiClient() as api_client:
        return await api_client.call_api(
            path,
            "POST",
            response_type=None,
            auth_settings=["BearerToken"],
            body={},
            _preload_content=False,
        )


async def patch_virtual_machine(namespace: str, name: str, patch: dict[str, Any]) -> None:
    if not patch:
        return

    await _ensure_kubeconfig()
    async with client.ApiClient() as api_client:
        custom = client.CustomObjectsApi(api_client)
        await custom.patch_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachines",
            name=name,
            body=patch,
        )

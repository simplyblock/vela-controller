from typing import Literal, cast

from fastapi import HTTPException
from kubernetes_asyncio import client, config

from .._util import StatusType

KubevirtSubresourceAction = Literal["pause", "unpause", "start", "stop"]


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


async def call_kubevirt_subresource(namespace: str, name: str, action: KubevirtSubresourceAction):
    await _ensure_kubeconfig()
    path = f"/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachines/{name}/{action}"
    print("calling kubevirt subresource", path)
    async with client.ApiClient() as api_client:
        return await api_client.call_api(
            path,
            "PUT",
            response_types_map={},
            auth_settings=["BearerToken"],
            body={},
            _preload_content=False,
        )
    raise HTTPException(status_code=404, detail=f"{action} not supported on this cluster")


async def get_virtualmachine_status(namespace: str, name: str) -> StatusType:
    """
    Fetch the current status of a VirtualMachine object.
    Looks for .status.printableStatus first, else falls back to UNKNOWN.
    """
    await _ensure_kubeconfig()

    custom_api = client.CustomObjectsApi()
    try:
        vm = await custom_api.get_namespaced_custom_object(
            group="kubevirt.io",
            version="v1",
            namespace=namespace,
            plural="virtualmachines",
            name=name,
        )
    except client.ApiException as e:
        if e.status == 404:
            raise HTTPException(status_code=404, detail=f"VirtualMachine {name} not found in {namespace}") from e
        raise

    # KubeVirt exposes status.printableStatus as a human readable state
    try:
        status = vm["status"]["printableStatus"]
        return cast("StatusType", status)
    except KeyError:
        pass  # Fall back to condition-based status check

    # Fallback: derive from conditions if printableStatus missing
    conditions = vm.get("status", {}).get("conditions", [])
    for cond in conditions:
        if cond.get("type") == "Ready" and cond.get("status") == "True":
            return cast("StatusType", "Running")

    return cast("StatusType", "UNKNOWN")

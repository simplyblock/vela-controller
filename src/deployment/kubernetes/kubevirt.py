from typing import Literal, cast

from aiohttp.client_exceptions import ClientError
from fastapi import HTTPException
from kubernetes_asyncio import client

from ..._util import StatusType
from ...exceptions import VelaKubernetesError
from ._util import api_client, custom_api_client

KubevirtSubresourceAction = Literal["pause", "unpause", "start", "stop"]


async def call_kubevirt_subresource(namespace: str, name: str, action: KubevirtSubresourceAction):
    path = f"/apis/subresources.kubevirt.io/v1/namespaces/{namespace}/virtualmachines/{name}/{action}"
    print("calling kubevirt subresource", path)
    async with api_client() as client:
        return await client.call_api(
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
    async with custom_api_client() as custom_client:
        try:
            vm = await custom_client.get_namespaced_custom_object(
                group="kubevirt.io",
                version="v1",
                namespace=namespace,
                plural="virtualmachines",
                name=name,
            )
        except (client.ApiException, ClientError, TimeoutError) as e:
            raise VelaKubernetesError("Failed to query virtual machine status") from e

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

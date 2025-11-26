from typing import Literal

from aiohttp.client_exceptions import ClientError
from kubernetes_asyncio.client.exceptions import ApiException

from ...exceptions import VelaKubernetesError
from ._util import custom_api_client

PowerState = Literal["Running", "Stopped"]


async def set_virtualmachine_power_state(namespace: str, name: str, power_state: PowerState) -> None:
    """
    Update the power state of a Neon VirtualMachine by patching its spec.powerState field.
    """
    try:
        async with custom_api_client() as custom_client:
            await custom_client.patch_namespaced_custom_object(
                group="vm.neon.tech",
                version="v1",
                namespace=namespace,
                plural="virtualmachines",
                name=name,
                body={"spec": {"powerState": power_state}},
                _content_type="application/merge-patch+json",
            )
    except ApiException:
        # Let Kubernetes API errors bubble up so callers can surface status codes (e.g. 404).
        raise
    except (ClientError, TimeoutError) as exc:
        raise VelaKubernetesError(
            f"Failed to set powerState={power_state} for Neon VM {name} in namespace {namespace}"
        ) from exc

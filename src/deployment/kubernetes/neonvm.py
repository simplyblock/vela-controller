from typing import Any, Literal

from aiohttp.client_exceptions import ClientError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ...exceptions import VelaKubernetesError
from ._util import custom_api_client

PowerState = Literal["Running", "Stopped"]


def to_camel(string: str) -> str:
    return "".join(word.capitalize() for word in string.split("_"))


class Status(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)

    phase: str
    pod_name: str


class NeonVM(BaseModel):
    spec: dict[str, Any] = Field(default_factory=dict)
    status: Status


async def get_neon_vm(namespace: str, name: str) -> NeonVM:
    """
    Fetch and validate a Neon VM custom object.
    """
    try:
        async with custom_api_client() as custom_client:
            vm_obj = await custom_client.get_namespaced_custom_object(
                group="vm.neon.tech",
                version="v1",
                namespace=namespace,
                plural="virtualmachines",
                name=name,
            )
    except Exception as exc:
        raise RuntimeError(f"Failed to fetch Neon VM {name!r} in namespace {namespace!r}") from exc

    try:
        return NeonVM.model_validate(vm_obj)
    except ValidationError as exc:
        raise RuntimeError(f"Failed to parse Neon VM for {name!r} in namespace {namespace!r}") from exc


async def resolve_autoscaler_vm_pod_name(namespace: str, vm_name: str) -> str:
    neon_vm = await get_neon_vm(namespace, vm_name)
    return neon_vm.status.pod_name


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

from typing import Any, Literal

from aiohttp.client_exceptions import ClientError
from kubernetes_asyncio.client.exceptions import ApiException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from ..._util import quantity_to_bytes, quantity_to_milli_cpu
from ...exceptions import VelaKubernetesError
from ._util import custom_api_client

PowerState = Literal["Running", "Stopped"]


def _require_int(value: Any, field: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise VelaKubernetesError(f"Autoscaler VM missing required integer field {field}") from None


def _require_quantity_bytes(value: Any, field: str) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        parsed = quantity_to_bytes(value)
        if parsed is not None:
            return parsed
    raise VelaKubernetesError(f"Autoscaler VM missing required quantity field {field}")


def _require_cpu_millis(value: Any, field: str) -> int:
    if isinstance(value, (int, float)):
        return int(value) if value >= 1000 else int(value * 1000)
    if isinstance(value, str):
        parsed = quantity_to_milli_cpu(value)
        if parsed is not None:
            return parsed
    raise VelaKubernetesError(f"Autoscaler VM missing required CPU field {field}")


def _extract_autoscaler_memory_bytes(slot_size_bytes: int, slot_count: int) -> int:
    return slot_size_bytes * slot_count


def _to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


class CamelModel(BaseModel):
    model_config = ConfigDict(alias_generator=_to_camel, populate_by_name=True)


class NeonVMStatus(CamelModel):
    phase: str
    pod_name: str = Field(default="", alias="podName")
    cpus: Any | None = None
    memory_slots_used: Any | None = None
    memory_slots: Any | None = None
    memory_size: Any | None = None

    @model_validator(mode="after")
    def ensure_slots(self: "NeonVMStatus") -> "NeonVMStatus":
        if self.memory_slots_used is None and self.memory_slots is None:
            raise VelaKubernetesError("Autoscaler VM missing status.memorySlots")
        return self

    @property
    def cpu_milli(self) -> int:
        return _require_cpu_millis(self.cpus, "status.cpus")

    @property
    def memory_slots_int(self) -> int:
        return _require_int(
            self.memory_slots_used or self.memory_slots,
            "status.memorySlots",
        )

    def memory_bytes(self, slot_size_bytes: int) -> int:
        if self.memory_size is not None:
            return _require_quantity_bytes(self.memory_size, "status.memorySize")
        return _extract_autoscaler_memory_bytes(slot_size_bytes, self.memory_slots_int)


class GuestCPUs(CamelModel):
    use: Any = Field(alias="use")  # you’d call _require_cpu_millis in a validator

    @property
    def use_milli(self) -> int:
        return _require_cpu_millis(self.use, "guest.cpus.use")


class MemorySlots(CamelModel):
    use: Any
    min: Any
    max: Any

    @property
    def use_int(self) -> int:
        return _require_int(self.use, "guest.memorySlots.use")

    @property
    def min_int(self) -> int:
        return _require_int(self.min, "guest.memorySlots.min")

    @property
    def max_int(self) -> int:
        return _require_int(self.max, "guest.memorySlots.max")


class Guest(CamelModel):
    cpus: GuestCPUs
    memory_slots: MemorySlots
    memory_slot_size: Any

    @property
    def slot_size_bytes(self) -> int:
        return _require_quantity_bytes(self.memory_slot_size, "guest.memorySlotSize")


class NeonVM(CamelModel):
    spec: dict[str, Any] = Field(default_factory=dict)
    status: NeonVMStatus

    @property
    def guest(self) -> Guest:
        guest = (self.spec or {}).get("guest") or {}
        return Guest.model_validate(guest)


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

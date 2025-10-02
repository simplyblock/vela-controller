import logging
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterable

from kubernetes_asyncio import client

from .kubevirt import _ensure_kubeconfig, call_kubevirt_subresource
from .settings import settings

logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = Decimal("0.15")
_CPU_MILLI = Decimal("1000")


def _parse_cpu_quantity(value: str | float | int | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))

    text = str(value).strip()
    if not text:
        return Decimal("0")
    if text.endswith("m"):
        return Decimal(text[:-1]) / _CPU_MILLI
    return Decimal(text)


def _extract_cpu_limit(vmi: dict[str, Any]) -> Decimal | None:
    try:
        resources = vmi["spec"]["domain"]["resources"]
    except KeyError:
        return None

    limits = resources.get("limits", {})
    limit_value = limits.get("cpu")
    if limit_value is None:
        requests = resources.get("requests", {})
        limit_value = requests.get("cpu")
    if limit_value is None:
        return None
    cpu = _parse_cpu_quantity(limit_value)
    return cpu if cpu > 0 else None


@dataclass
class _VMIPlacement:
    namespace: str
    name: str
    node: str
    cpu_limit: Decimal


async def _list_branch_vmis(namespace_prefix: str) -> list[_VMIPlacement]:
    await _ensure_kubeconfig()
    async with client.ApiClient() as api_client:
        custom = client.CustomObjectsApi(api_client)
        response = await custom.list_cluster_custom_object(
            group="kubevirt.io",
            version="v1",
            plural="virtualmachineinstances",
        )

    placements: list[_VMIPlacement] = []
    for item in response.get("items", []):
        metadata = item.get("metadata", {})
        namespace = metadata.get("namespace")
        name = metadata.get("name")
        if not namespace or not name:
            continue
        if not namespace.startswith(namespace_prefix):
            continue

        status = item.get("status", {})
        if status.get("phase") != "Running":
            continue
        node = status.get("nodeName")
        if not node:
            continue

        cpu_limit = _extract_cpu_limit(item)
        if cpu_limit is None:
            continue

        placements.append(_VMIPlacement(namespace=namespace, name=name, node=node, cpu_limit=cpu_limit))

    return placements


def _nodes_by_load(placements: Iterable[_VMIPlacement]) -> dict[str, Decimal]:
    loads: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for placement in placements:
        loads[placement.node] += placement.cpu_limit
    return loads


async def rebalance_virtual_machines(
    *,
    namespace_prefix: str | None = None,
    imbalance_threshold: Decimal = _DEFAULT_THRESHOLD,
) -> list[_VMIPlacement]:
    target_prefix = namespace_prefix or f"{settings.deployment_namespace_prefix}-deployment-"
    placements = await _list_branch_vmis(target_prefix)
    if len(placements) <= 1:
        return []

    loads = _nodes_by_load(placements)
    if len(loads) <= 1:
        return []

    total_load = sum(loads.values())
    if total_load == 0:
        return []

    average = total_load / Decimal(len(loads))
    upper_bound = average * (Decimal("1") + imbalance_threshold)
    lower_bound = average * (Decimal("1") - imbalance_threshold)

    overloaded = {node for node, load in loads.items() if load > upper_bound}
    underloaded = {node for node, load in loads.items() if load < lower_bound}

    if not overloaded or not underloaded:
        return []

    placements_by_node: dict[str, list[_VMIPlacement]] = defaultdict(list)
    for placement in placements:
        placements_by_node[placement.node].append(placement)

    migrations: list[_VMIPlacement] = []
    for node in sorted(overloaded, key=lambda n: loads[n], reverse=True):
        candidates = sorted(placements_by_node[node], key=lambda p: p.cpu_limit, reverse=True)
        for candidate in candidates:
            if loads[node] <= upper_bound:
                break
            logger.info(
                "Initiating live migration of %s/%s from node %s (load=%s, avg=%s)",
                candidate.namespace,
                candidate.name,
                node,
                loads[node],
                average,
            )
            try:
                await call_kubevirt_subresource(candidate.namespace, candidate.name, "migrate")
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Live migration request for %s/%s failed: %s",
                    candidate.namespace,
                    candidate.name,
                    exc,
                )
                continue
            loads[node] -= candidate.cpu_limit
            migrations.append(candidate)
            break

    return migrations

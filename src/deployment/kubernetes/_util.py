import inspect
from collections.abc import Mapping
from contextlib import asynccontextmanager

from aiohttp import ClientTimeout
from kubernetes_asyncio.client import ApiClient, CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.config import load_incluster_config, load_kube_config
from kubernetes_asyncio.config.config_exception import ConfigException

from ...exceptions import VelaKubernetesError

_API_CLIENT_SIGNATURE = inspect.signature(ApiClient.call_api)
_SUPPORTS_RESPONSE_TYPE = "response_type" in _API_CLIENT_SIGNATURE.parameters
_SUPPORTS_RESPONSE_TYPES_MAP = "response_types_map" in _API_CLIENT_SIGNATURE.parameters


def _to_response_types_map(response_type: object) -> dict[int | str, object]:
    """
    Normalize the legacy `response_type` argument into the newer response_types_map format.
    Returns a mapping keyed by 200 when a simple type is supplied, mirroring the previous behaviour.
    """
    if response_type is None:
        return {}
    if isinstance(response_type, Mapping):
        return dict(response_type)
    return {200: response_type}


def _to_response_type(response_types_map: object) -> object:
    """
    Collapse a `response_types_map` back to the first non-None entry for legacy clients
    that only understand the single `response_type` argument.
    """
    if not response_types_map:
        return None
    if isinstance(response_types_map, Mapping):
        for value in response_types_map.values():
            if value is not None:
                return value
        return None
    return response_types_map


class ApiClientWithTimeout(ApiClient):
    def __init__(self, *args, default_timeout=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_timeout = default_timeout

    async def call_api(self, *args, _request_timeout=None, **kwargs):
        if _request_timeout is None:
            _request_timeout = self.default_timeout
        if "_request_timeout" not in kwargs:
            kwargs["_request_timeout"] = _request_timeout
        if not _SUPPORTS_RESPONSE_TYPE and "response_type" in kwargs and "response_types_map" not in kwargs:
            kwargs["response_types_map"] = _to_response_types_map(kwargs.pop("response_type"))
        if not _SUPPORTS_RESPONSE_TYPES_MAP and "response_types_map" in kwargs and "response_type" not in kwargs:
            kwargs["response_type"] = _to_response_type(kwargs.pop("response_types_map"))
        return await super().call_api(*args, **kwargs)


async def _ensure_kubeconfig() -> None:
    try:
        load_incluster_config()
    except ConfigException:
        try:
            await load_kube_config()
        except ConfigException as e:
            raise VelaKubernetesError("Kubernetes client not configured. Mount kubeconfig or run in-cluster.") from e


@asynccontextmanager
async def api_client():
    await _ensure_kubeconfig()
    async with ApiClientWithTimeout(default_timeout=2) as api_client:
        api_client.rest_client.pool_manager._timeout = ClientTimeout(sock_connect=2)
        yield api_client


@asynccontextmanager
async def core_v1_client():
    async with api_client() as client:
        yield CoreV1Api(api_client=client)


@asynccontextmanager
async def custom_api_client():
    async with api_client() as client:
        yield CustomObjectsApi(api_client=client)

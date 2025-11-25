from contextlib import asynccontextmanager

from aiohttp import ClientTimeout
from kubernetes_asyncio.client import ApiClient, AppsV1Api, CoreV1Api, CustomObjectsApi, StorageV1Api
from kubernetes_asyncio.config import load_incluster_config, load_kube_config
from kubernetes_asyncio.config.config_exception import ConfigException

from ...exceptions import VelaKubernetesError

KUBE_API_SERVER_TIMEOUT = 10


class ApiClientWithTimeout(ApiClient):
    def __init__(self, *args, default_timeout=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_timeout = default_timeout

    async def call_api(self, *args, _request_timeout=None, **kwargs):
        if _request_timeout is None:
            _request_timeout = self.default_timeout
        if "_request_timeout" not in kwargs:
            kwargs["_request_timeout"] = _request_timeout
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
    async with ApiClientWithTimeout(default_timeout=KUBE_API_SERVER_TIMEOUT) as api_client:
        api_client.rest_client.pool_manager._timeout = ClientTimeout(sock_connect=KUBE_API_SERVER_TIMEOUT)
        yield api_client


@asynccontextmanager
async def core_v1_client():
    async with api_client() as client:
        yield CoreV1Api(api_client=client)


@asynccontextmanager
async def custom_api_client():
    async with api_client() as client:
        yield CustomObjectsApi(api_client=client)


@asynccontextmanager
async def storage_v1_client():
    async with api_client() as client:
        yield StorageV1Api(api_client=client)


@asynccontextmanager
async def apps_v1_client():
    async with api_client() as client:
        yield AppsV1Api(api_client=client)

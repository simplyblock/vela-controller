from contextlib import asynccontextmanager

from kubernetes_asyncio.client import ApiClient, CoreV1Api, CustomObjectsApi
from kubernetes_asyncio.config import load_incluster_config, load_kube_config
from kubernetes_asyncio.config.config_exception import ConfigException

from ...exceptions import VelaKubernetesError


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
    async with ApiClient() as api_client:
        yield api_client


@asynccontextmanager
async def core_v1_client():
    async with api_client() as client:
        yield CoreV1Api(api_client=client)


@asynccontextmanager
async def custom_api_client():
    async with api_client() as client:
        yield CustomObjectsApi(api_client=client)

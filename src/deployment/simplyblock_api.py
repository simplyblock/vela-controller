from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    import httpx

SIMPLYBLOCK_API_TIMEOUT_SECONDS = 10.0
SIMPLYBLOCK_STORAGE_POOL_NAME = "testing1"


class SimplyblockApi:
    def __init__(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        cluster_id: str,
        cluster_secret: str,
    ) -> None:
        self._client = client
        self._endpoint = endpoint.rstrip("/")
        self._cluster_id = cluster_id
        self._cluster_secret = cluster_secret
        self._pool_id_cache: dict[str, UUID] = {}

    @property
    def _cluster_base(self) -> str:
        return f"{self._endpoint}/api/v2/clusters/{self._cluster_id}"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cluster_secret}",
            "Accept": "application/json",
        }

    async def _cluster_pool_base(self) -> str:
        pool_id = await self.pool_id()
        return f"{self._endpoint}/api/v2/clusters/{self._cluster_id}/storage-pools/{pool_id}"

    async def pool(self, name: str = SIMPLYBLOCK_STORAGE_POOL_NAME) -> dict[str, Any]:
        url = f"{self._cluster_base}/storage-pools/"
        response = await self._client.get(url, headers=self._headers(), timeout=SIMPLYBLOCK_API_TIMEOUT_SECONDS)
        response.raise_for_status()

        pools = response.json()
        if isinstance(pools, list):
            for pool in pools:
                if isinstance(pool, dict) and pool.get("name") == name:
                    return pool
        raise KeyError(f"Storage pool {name!r} not found")

    async def pool_id(self, name: str = SIMPLYBLOCK_STORAGE_POOL_NAME) -> UUID:
        cached = self._pool_id_cache.get(name)
        if cached:
            return cached
        pool = await self.pool(name)
        identifier = UUID(str(pool["id"]))
        self._pool_id_cache[name] = identifier
        return identifier

    async def volume_iostats(self, volume_uuid: str) -> dict[str, Any]:
        base_url = await self._cluster_pool_base()
        url = f"{base_url}/volumes/{volume_uuid}/iostats"
        response = await self._client.get(url, headers=self._headers(), timeout=SIMPLYBLOCK_API_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        return payload[0]  # return the most recent one

    async def update_volume(
        self,
        volume_uuid: str,
        payload: dict[str, Any],
    ) -> None:
        headers = self._headers()
        headers["Content-Type"] = "application/json"
        base_url = await self._cluster_pool_base()
        url = f"{base_url}/volumes/{volume_uuid}"
        response = await self._client.put(
            url,
            headers=headers,
            json=payload,
            timeout=SIMPLYBLOCK_API_TIMEOUT_SECONDS,
        )
        response.raise_for_status()


async def create_simplyblock_api(client: httpx.AsyncClient) -> SimplyblockApi:
    from . import load_simplyblock_credentials

    endpoint, cluster_id, cluster_secret = await load_simplyblock_credentials()
    return SimplyblockApi(
        client=client,
        endpoint=endpoint,
        cluster_id=cluster_id,
        cluster_secret=cluster_secret,
    )

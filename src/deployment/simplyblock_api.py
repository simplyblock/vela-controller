from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any
from uuid import UUID

import httpx

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class SimplyblockApi:
    API_TIMEOUT_SECONDS: float = 10.0
    STORAGE_POOL_NAME: str = "testing1"

    def __init__(
        self,
        endpoint: str,
        cluster_id: str,
        cluster_secret: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float | httpx.Timeout | None = None,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._cluster_id = cluster_id
        self._cluster_secret = cluster_secret
        self._pool_id_cache: dict[str, UUID] = {}
        fallback_timeout = client.timeout if client is not None else self.API_TIMEOUT_SECONDS
        self._timeout = timeout if timeout is not None else fallback_timeout
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=self._endpoint,
            headers=self._headers(),
            timeout=self._timeout,
        )

    async def __aenter__(self) -> SimplyblockApi:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

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
        return f"{self._cluster_base}/storage-pools/{pool_id}"

    async def pool(self, name: str | None = None) -> dict[str, Any]:
        pool_name = name or self.STORAGE_POOL_NAME
        url = f"{self._cluster_base}/storage-pools/"
        response = await self._client.get(url, headers=self._headers(), timeout=self._timeout)
        response.raise_for_status()

        pools = response.json()
        if isinstance(pools, list):
            for pool in pools:
                if isinstance(pool, dict) and pool.get("name") == pool_name:
                    return pool
        raise KeyError(f"Storage pool {pool_name!r} not found")

    async def pool_id(self, name: str | None = None) -> UUID:
        pool_name = name or self.STORAGE_POOL_NAME
        cached = self._pool_id_cache.get(pool_name)
        if cached:
            return cached
        pool = await self.pool(pool_name)
        identifier = UUID(str(pool["id"]))
        self._pool_id_cache[pool_name] = identifier
        return identifier

    async def volume_iostats(self, volume_uuid: str) -> dict[str, Any]:
        base_url = await self._cluster_pool_base()
        url = f"{base_url}/volumes/{volume_uuid}/iostats"
        response = await self._client.get(url, headers=self._headers(), timeout=self._timeout)
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
        url = f"{base_url}/volumes/{volume_uuid}/"
        response = await self._client.put(
            url,
            headers=headers,
            json=payload,
            timeout=self._timeout,
        )
        response.raise_for_status()


@asynccontextmanager
async def create_simplyblock_api(
    client: httpx.AsyncClient | None = None,
) -> AsyncIterator[SimplyblockApi]:
    from . import load_simplyblock_credentials

    endpoint, cluster_id, cluster_secret = await load_simplyblock_credentials()
    api = SimplyblockApi(
        endpoint=endpoint,
        cluster_id=cluster_id,
        cluster_secret=cluster_secret,
        client=client,
    )
    async with api:
        yield api

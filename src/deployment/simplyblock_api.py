from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Self
from uuid import UUID

import httpx

from ..exceptions import VelaSimplyblockAPIError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


logger = logging.getLogger(__name__)


class SimplyblockPoolApi:
    API_TIMEOUT_SECONDS: float = 10.0

    def __init__(
        self,
        endpoint: str,
        cluster_id: UUID,
        cluster_secret: str,
        pool_name: str,
        *,
        timeout: float | httpx.Timeout = API_TIMEOUT_SECONDS,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._cluster_id = cluster_id
        self._cluster_secret = cluster_secret
        self._pool_id_cache: dict[str, UUID] = {}
        self._pool_name = pool_name
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> Self:
        if self._client is not None:
            raise RuntimeError("Cannot open instance repeatedly")

        self._client = await httpx.AsyncClient(
            base_url=self._endpoint,
            headers=self._headers(),
            timeout=self._timeout,
        ).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client is None:
            return

        await self._client.__aexit__(exc_type, exc_val, exc_tb)
        self._client = None

    @property
    def _cluster_base(self) -> str:
        return f"{self._endpoint}/api/v2/clusters/{self._cluster_id}"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cluster_secret}",
            "Accept": "application/json",
        }

    async def _cluster_pool_base(self) -> str:
        pool_id = await self.pool_id(self._pool_name)
        return f"{self._cluster_base}/storage-pools/{pool_id}"

    async def pool(self, name: str) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Cannot use unopened instance")

        url = f"{self._cluster_base}/storage-pools/"
        response = await self._client.get(url)
        response.raise_for_status()

        pools = response.json()
        if isinstance(pools, list):
            for pool in pools:
                if isinstance(pool, dict) and pool.get("name") == name:
                    return pool
        raise KeyError(f"Storage pool {name!r} not found")

    async def pool_id(self, name: str) -> UUID:
        if self._client is None:
            raise RuntimeError("Cannot use unopened instance")

        cached = self._pool_id_cache.get(name)
        if cached:
            return cached
        pool = await self.pool(name)
        identifier = UUID(str(pool["id"]))
        self._pool_id_cache[name] = identifier
        return identifier

    async def volume_iostats(self, volume_uuid: str) -> dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Cannot use unopened instance")

        base_url = await self._cluster_pool_base()
        url = f"{base_url}/volumes/{volume_uuid}/iostats"
        response = await self._client.get(url)
        response.raise_for_status()
        payload = response.json()
        if len(payload) == 0:
            raise VelaSimplyblockAPIError(f"Empty iostats payload for volume {volume_uuid}")
        return payload[0]

    async def update_volume(
        self,
        volume_uuid: str,
        payload: dict[str, Any],
    ) -> None:
        if self._client is None:
            raise RuntimeError("Cannot use unopened instance")

        base_url = await self._cluster_pool_base()
        url = f"{base_url}/volumes/{volume_uuid}/"
        response = await self._client.put(url, json=payload)
        response.raise_for_status()


@asynccontextmanager
async def create_simplyblock_api() -> AsyncIterator[SimplyblockPoolApi]:
    from . import load_simplyblock_credentials

    api = SimplyblockPoolApi(*(await load_simplyblock_credentials()), "testing1")
    async with api:
        yield api

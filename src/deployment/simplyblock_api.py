from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Self
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..exceptions import VelaSimplyblockAPIError

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


logger = logging.getLogger(__name__)


class SimplyblockVolume(BaseModel):
    model_config = ConfigDict(extra="ignore")

    size: int = Field(gt=0)


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
        self._cluster_id = cluster_id
        self._cluster_secret = cluster_secret
        self._pool_id_cache: dict[str, UUID] = {}
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

        storage_pools_url = f"{endpoint}/api/v2/clusters/{self._cluster_id}/storage-pools/"
        try:
            response = httpx.get(storage_pools_url, headers=self._headers(), timeout=self._timeout)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise VelaSimplyblockAPIError("Failed to retrieve storage pools") from e

        pool_id = next((UUID(pool["id"]) for pool in response.json() if pool.get("name") == pool_name), None)
        if pool_id is None:
            raise VelaSimplyblockAPIError(f"Failed to retrieve storage pool {pool_name}")

        self._base_url = storage_pools_url + f"{pool_id}/"

    async def __aenter__(self) -> Self:
        if self._client is not None:
            raise RuntimeError("Cannot open instance repeatedly")

        self._client = await httpx.AsyncClient(
            base_url=self._base_url,
            headers=self._headers(),
            timeout=self._timeout,
        ).__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._client is None:
            return

        await self._client.__aexit__(exc_type, exc_val, exc_tb)
        self._client = None

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cluster_secret}",
            "Accept": "application/json",
        }

    async def _get(self, url) -> dict | list:
        if self._client is None:
            raise RuntimeError("Cannot use unopened instance")

        try:
            response = await self._client.get(url)
            response.raise_for_status()
            result = response.json()
        except httpx.HTTPError as e:
            raise VelaSimplyblockAPIError("Request failed") from e

        return result

    async def _put(self, url, data) -> None:
        if self._client is None:
            raise RuntimeError("Cannot use unopened instance")

        try:
            response = await self._client.put(url, json=data)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise VelaSimplyblockAPIError("Request failed") from e

    async def volume_iostats(self, volume: UUID) -> dict[str, Any]:
        iostats = await self._get(f"volumes/{volume}/iostats")
        if len(iostats) == 0:
            raise VelaSimplyblockAPIError(f"Empty iostats payload for volume {volume}")
        return iostats[0]

    async def update_volume(
        self,
        volume: UUID,
        payload: dict[str, Any],
    ) -> None:
        await self._put(f"volumes/{volume}/", data=payload)

    async def get_volume(
        self,
        volume: UUID,
    ) -> SimplyblockVolume:
        volume_payload = await self._get(f"volumes/{volume}/")
        if not isinstance(volume_payload, dict):
            raise VelaSimplyblockAPIError(f"Unexpected volume payload for volume {volume}")
        try:
            return SimplyblockVolume.model_validate(volume_payload)
        except ValidationError as exc:
            raise VelaSimplyblockAPIError(f"Invalid volume payload for volume {volume}") from exc


@asynccontextmanager
async def create_simplyblock_api() -> AsyncIterator[SimplyblockPoolApi]:
    from . import load_simplyblock_credentials

    api = SimplyblockPoolApi(*(await load_simplyblock_credentials()))
    async with api:
        yield api

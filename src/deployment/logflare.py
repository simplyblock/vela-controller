import logging
from typing import List
import httpx
from fastapi import HTTPException, Request, status

from .._util import Identifier
from ..exceptions import VelaLogflareError
from .settings import settings

logger = logging.getLogger(__name__)

LOGFLARE_URL = settings.logflare_url
LOGFLARE_API_KEY = settings.logflare_private_access_token

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {LOGFLARE_API_KEY}",
}


# --- SOURCE CREATION ---
async def create_sources(branch_id: str) -> List[str]:
    """
    Create multiple Logflare sources for the given branch.
    Returns a list of successfully created source names.
    """
    source_names = [
        "realtime.logs.prod",
        "postgREST.logs.prod",
        "postgres.logs",
        "deno-relay-logs",
        "storage.logs.prod.2",
    ]

    created_sources = []

    async with httpx.AsyncClient(timeout=10) as client:
        for name in source_names:
            full_name = f"{branch_id}.{name}"
            payload = {"name": full_name}

            try:
                response = await client.post(f"{LOGFLARE_URL}/api/sources", headers=headers, json=payload)
                response.raise_for_status()
                logger.info(f"Logflare source '{full_name}' created successfully.")
                created_sources.append(full_name)
            except httpx.HTTPStatusError as exc:
                logger.error(
                    f"Failed to create Logflare source '{full_name}': "
                    f"{exc.response.status_code} {exc.response.reason_phrase} | {exc.response.text}"
                )
            except httpx.RequestError as exc:
                logger.error(f"Request error while creating source '{full_name}': {exc}")

    return created_sources

# --- ENDPOINT CREATION ---
async def create_endpoint(branch_id: Identifier, endpoint_name: str, enable_auth: bool = True) -> str:
    async with httpx.AsyncClient() as client:
        try:
            payload = {
                "description": f"endpoint for branch {branch_id}",
                "enable_auth": enable_auth,
                "name": f"{branch_id}_{endpoint_name}",
                "query": f"select id, event_message, timestamp from `{branch_id}_source`",
                "sandboxable": True,
            }

            response = await client.post(f"{LOGFLARE_URL}/api/endpoints", headers=headers, json=payload)
            response.raise_for_status()

            logger.info(f"Logflare endpoint '{payload['name']}' created successfully.")
            return response.json().get("id") or response.json().get("endpoint_id")
            
        except httpx.HTTPStatusError as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)

            if status_code == 409:
                logger.warning(f"Endpoint '{branch_id}_{endpoint_name}' already exists.")
            else:
                logger.error(f"Failed to create Logflare endpoint '{endpoint_name}': {exc}")

            raise VelaLogflareError(f"Failed to create Logflare endpoint '{endpoint_name}': {exc}") from exc

        except Exception as exc:
            logger.exception(f"Unexpected error creating Logflare endpoint '{endpoint_name}'.")
            raise VelaLogflareError(f"Unexpected error creating Logflare endpoint: {exc}") from exc


# --- LOG QUERY ---
async def get_logs(branch_id: Identifier, endpoint_name: str, pg_sql_query: str):
    async with httpx.AsyncClient() as client:
        try:
            url = f"{LOGFLARE_URL}/api/endpoints/query/{branch_id}_{endpoint_name}"
            params = {"pg_sql": pg_sql_query}
            response = await client.get(url, headers=headers, params=params)
            response.raise_for_status()

            logger.info(f"Fetched logs for endpoint '{branch_id}_{endpoint_name}'.")
            return response.json()

        except httpx.HTTPStatusError as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)

            if status_code == 404:
                logger.warning(f"Logs endpoint '{branch_id}_{endpoint_name}' not found.")
            elif status_code == 401:
                logger.warning("Unauthorized Logflare query request.")
            else:
                logger.error(f"HTTP error fetching logs for '{endpoint_name}': {exc}")

            raise VelaLogflareError(f"Failed to fetch logs for '{endpoint_name}': {exc}") from exc

        except Exception as exc:
            logger.exception(f"Unexpected error fetching logs for endpoint '{endpoint_name}'.")
            raise VelaLogflareError(f"Unexpected error fetching logs: {exc}") from exc


# --- COMPOSITE CREATION ---
async def create_logflare_objects(branch_id: Identifier):
    """
    Creates a Logflare source and endpoint for a given branch.
    """
    logger.info(f"Creating Logflare objects for branch_id={branch_id}")

    source_id = await create_source(str(branch_id), "source")
    endpoint_id = await create_endpoint(str(branch_id), "endpoint")

    logger.info(f"Created Logflare source {source_id} and endpoint {endpoint_id} for branch {branch_id}.")
    #return {"source_id": source_id, "endpoint_id": endpoint_id}

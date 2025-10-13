import logging
import httpx
from fastapi import HTTPException, Request, status

from .._util import Identifier
from ..exceptions import VelaLogflareError
from .settings import settings

logger = logging.getLogger(__name__)

LOGFLARE_URL = settings.logflare_url
LOGFLARE_API_KEY = settings.logflare_public_access_token

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {LOGFLARE_API_KEY}",
}


# --- SOURCE CREATION ---
async def create_source(branch_id: Identifier, source_name: str) -> str:
    async with httpx.AsyncClient() as client:
        try:
            payload = {"name": f"{branch_id}_{source_name}"}
            response = await client.post(f"{LOGFLARE_URL}/api/sources", headers=headers, json=payload)
            response.raise_for_status()

            logger.info(f"Logflare source '{payload['name']}' created successfully.")
            return response.json().get("id") or response.json().get("source_id")

        except httpx.HTTPStatusError as exc:
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)

            if status_code == 409:
                logger.warning(f"Source '{branch_id}_{source_name}' already exists.")
                # Optionally fetch existing sources here if needed
            else:
                logger.error(f"Failed to create Logflare source '{source_name}': {exc}")

            raise VelaLogflareError(f"Failed to create Logflare source '{source_name}': {exc}") from exc

        except Exception as exc:
            logger.exception(f"Unexpected error creating Logflare source '{source_name}'.")
            raise VelaLogflareError(f"Unexpected error creating Logflare source: {exc}") from exc


# --- ENDPOINT CREATION ---
async def create_endpoint(branch_id: Identifier, endpoint_name: str, enable_auth: bool = True) -> str:
    async with httpx.AsyncClient() as client:
        try:
            payload = {
                "description": f"endpoint for branch {branch_id}",
                "enable_auth": enable_auth,
                "name": f"{branch_id}_{endpoint_name}",
                "query": "",
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
async def create_logflare_objects(branch_id: Identifier, request: Request):
    """
    Creates a Logflare source and endpoint for a given branch.
    """
    logger.info(f"Creating Logflare objects for branch_id={branch_id}")

    source_id = await create_source(branch_id, "source")
    endpoint_id = await create_endpoint(branch_id, "endpoint")

    logger.info(f"Created Logflare source {source_id} and endpoint {endpoint_id} for branch {branch_id}.")
    return {"source_id": source_id, "endpoint_id": endpoint_id}

import logging

import httpx

from .._util import Identifier
from .settings import settings

logger = logging.getLogger(__name__)

LOGFLARE_API_KEY = settings.logflare_private_access_token

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {LOGFLARE_API_KEY}",
}


# --- SOURCE CREATION ---
async def create_sources(branch_id: str, *, create_default_sources: bool = False) -> list[str]:
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

    if create_default_sources:
        source_names = [
            "auth.logs.vela",
            "controller.logs.vela",
            "studio.logs.vela",
            "db.logs.vela",
            "kong.logs.vela",
        ]

    created_sources = []

    async with httpx.AsyncClient(timeout=10) as client:
        for name in source_names:
            full_name = f"{branch_id}.{name}"
            payload = {"name": full_name}

            try:
                response = await client.post(f"{settings.logflare_url}/api/sources", headers=headers, json=payload)
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
async def create_all_logs_endpoint(branch_id: str) -> str:
    """
    Creates a single endpoint aggregating all branch sources into one query.
    """
    endpoint_name = f"{branch_id}.logs.all"

    sql_query = f"""
    WITH realtime_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `{branch_id}.realtime.logs.prod` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
      WHERE m.project = @project
    ),
    postgrest_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `{branch_id}.postgREST.logs.prod` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
      WHERE t.project = @project
    ),
    postgres_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `{branch_id}.postgres.logs` AS t
      WHERE t.project = @project
    ),
    deno_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `{branch_id}.deno-relay-logs` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
      WHERE m.project_ref = @project
    ),
    storage_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `{branch_id}.storage.logs.prod.2` AS t
      CROSS JOIN UNNEST(t.metadata) AS m
      WHERE m.project = @project
    )
    SELECT id, timestamp, event_message, metadata
    FROM realtime_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM postgrest_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM postgres_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM deno_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM storage_logs
    ORDER BY CAST(timestamp AS timestamp) DESC
    LIMIT 100;
    """

    payload = {
        "name": endpoint_name,
        "description": f"Aggregated all logs endpoint for branch {branch_id}",
        "enable_auth": True,
        "sandboxable": True,
        "query": sql_query.strip(),
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{settings.logflare_url}/api/endpoints",
                headers=headers,
                json=payload,
                timeout=60,
            )
            response.raise_for_status()
            logger.info(f"Created Logflare endpoint '{endpoint_name}' for branch {branch_id}")
            return response.json().get("id") or response.json().get("endpoint_id")
        except httpx.HTTPStatusError as exc:
            logger.error(f"Failed to create Logflare endpoint '{endpoint_name}': {exc.response.text}")
            raise


# --- LOG QUERY ---
async def get_logs_from_endpoint(branch_id: str, source: str, limit: int = 100):
    """
    Query logs for a specific source using the branch endpoint.
    Example source: 'postgres.logs' or 'realtime.logs.prod'
    """
    pg_sql_query = f"""
    SELECT timestamp, id, event_message, metadata
    FROM "{branch_id}.{source}"
    ORDER BY CAST(timestamp AS timestamp) DESC
    LIMIT {limit};
    """

    endpoint_name = f"{branch_id}.logs.all"

    url = f"{settings.logflare_url}/api/endpoints/query/{endpoint_name}"

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(
                url,
                headers=headers,
                params={"pg_sql": pg_sql_query, "project": branch_id},
            )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Retrieved {len(data)} logs from {source} via endpoint '{endpoint_name}'")
            return data
        except httpx.HTTPStatusError as exc:
            logger.error(f"Failed to fetch logs for '{source}': {exc.response.text}")
            raise


# --- SOURCE DELETION ---
async def delete_sources(branch_id: str) -> None:
    """
    Delete all Logflare sources for the given branch.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            list_resp = await client.get(f"{settings.logflare_url}/api/sources", headers=headers)
            list_resp.raise_for_status()
            sources = list_resp.json()

            branch_sources = [s for s in sources if s["name"].startswith(f"{branch_id}.")]

            if not branch_sources:
                logger.info(f"No sources found for branch {branch_id}")
                return

            for src in branch_sources:
                src_id = src.get("id")
                src_name = src.get("name")
                if not src_id:
                    logger.warning(f"Skipping source '{src_name}' without ID.")
                    continue

                delete_url = f"{settings.logflare_url}/api/sources/{src_id}"
                try:
                    del_resp = await client.delete(delete_url, headers=headers)
                    del_resp.raise_for_status()
                    logger.info(f"Deleted Logflare source '{src_name}' (id={src_id})")
                except httpx.HTTPStatusError as exc:
                    logger.error(f"Failed to delete source '{src_name}': {exc.response.text}")

        except httpx.RequestError as exc:
            logger.error(f"Request error while deleting sources for branch '{branch_id}': {exc}")
        except httpx.HTTPStatusError as exc:
            logger.error(f"Failed to list sources for branch '{branch_id}': {exc.response.text}")

# --- ENDPOINT DELETION ---
async def delete_endpoint(branch_id: str) -> None:
    """
    Delete the aggregated endpoint for a given branch.
    """
    endpoint_name = f"{branch_id}.logs.all"

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            # List all endpoints to find the ID
            list_resp = await client.get(f"{settings.logflare_url}/api/endpoints", headers=headers)
            list_resp.raise_for_status()
            endpoints = list_resp.json()

            endpoint = next((e for e in endpoints if e["name"] == endpoint_name), None)
            if not endpoint:
                logger.info(f"No endpoint found for branch {branch_id}")
                return
                
            endpoint_id = endpoint.get("id")
            delete_url = f"{settings.logflare_url}/api/endpoints/{endpoint_id}"

            del_resp = await client.delete(delete_url, headers=headers)
            del_resp.raise_for_status()
            logger.info(f"Deleted Logflare endpoint '{endpoint_name}' (id={endpoint_id})")

        except httpx.RequestError as exc:
            logger.error(f"Request error while deleting endpoint for branch '{branch_id}': {exc}")
        except httpx.HTTPStatusError as exc:
            logger.error(f"Failed to delete endpoint for branch '{branch_id}': {exc.response.text}")

async def create_logflare_objects(branch_id: Identifier):
    """
    Creates a Logflare source and endpoint for a given branch.
    """
    logger.info(f"Creating Logflare objects for branch_id={branch_id}")

    source_id = await create_sources(str(branch_id))
    endpoint_id = await create_all_logs_endpoint(str(branch_id))

    logger.info(f"Created Logflare source {source_id} and endpoint {endpoint_id} for branch {branch_id}.")

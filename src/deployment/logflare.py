import logging

import httpx

from .._util import Identifier
from ..exceptions import VelaLogflareError
from .settings import settings

logger = logging.getLogger(__name__)

LOGFLARE_API_KEY = settings.logflare_private_access_token

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Bearer {LOGFLARE_API_KEY}",
}


# --- INTERNAL HELPER ---
async def _create_sources(sources: list[str], prefix: str | None = None) -> list[str]:
    """
    Internal helper for creating Logflare sources.
    If a prefix (e.g., branch_id) is provided, it prefixes each source name.
    Returns a list of successfully created or existing source names.
    Raises VelaLogflareError on unrecoverable failures.
    """
    created_sources = []

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            # Fetch existing sources once to avoid repeated calls
            list_resp = await client.get(f"{settings.logflare_url}/api/sources", headers=headers)
            list_resp.raise_for_status()
            existing_sources = {s["name"] for s in list_resp.json()}

        except httpx.HTTPError as exc:
            raise VelaLogflareError("Failed to list existing Logflare sources") from exc

        for name in sources:
            full_name = f"{prefix}.{name}" if prefix else name

            # Skip creation if already exists
            if full_name in existing_sources:
                logger.info(f"Logflare source '{full_name}' already exists. Skipping creation.")
                created_sources.append(full_name)
                continue

            payload = {"name": full_name}
            try:
                response = await client.post(
                    f"{settings.logflare_url}/api/sources",
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                logger.info(f"Logflare source '{full_name}' created successfully.")
                created_sources.append(full_name)

            except httpx.HTTPError as exc:
                raise VelaLogflareError(f"Failed to create Logflare source '{full_name}'") from exc

    return created_sources


# --- BRANCH SOURCES ---
async def create_branch_sources(branch_id: str) -> list[str]:
    """
    Create branch-specific Logflare sources.
    Example: "main.realtime.logs.prod"
    """
    branch_sources = [
        "realtime.logs.prod",
        "postgREST.logs.prod",
        "postgres.logs",
        "deno-relay-logs",
        "storage.logs.prod.2",
    ]
    return await _create_sources(branch_sources, prefix=branch_id)


# --- GLOBAL SOURCES ---
async def create_global_sources() -> list[str]:
    """
    Create global (default) Logflare sources shared across all branches.
    Example: "auth.logs.vela"
    """
    global_sources = [
        "global.auth.logs.vela",
        "global.controller.logs.vela",
        "global.studio.logs.vela",
        "global.db.logs.vela",
        "global.kong.logs.vela",
    ]
    return await _create_sources(global_sources)


# --- ENDPOINT CREATION ---
async def _create_endpoint(
    name: str,
    description: str,
    sql_query: str,
) -> str:
    """
    Internal helper to create a Logflare endpoint.
    Checks if the endpoint already exists before attempting creation.
    If it exists, returns its existing ID.
    Raises VelaLogflareError on any unrecoverable failure.
    """
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            res = await client.get(f"{settings.logflare_url}/api/endpoints", headers=headers)
            res.raise_for_status()
            endpoints = res.json()

            for e in endpoints:
                if e.get("name") == name:
                    endpoint_id = e.get("id")
                    logger.info(f"Endpoint '{name}' already exists (id={endpoint_id}). Skipping creation.")
                    return endpoint_id

            payload = {
                "name": name,
                "description": description,
                "enable_auth": True,
                "sandboxable": True,
                "query": sql_query.strip(),
            }

            response = await client.post(
                f"{settings.logflare_url}/api/endpoints",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            endpoint_id = data.get("id") or data.get("endpoint_id")
            logger.info(f"Created Logflare endpoint '{name}' successfully (id={endpoint_id}).")
            return endpoint_id

        except httpx.HTTPError as exc:
            raise VelaLogflareError(f"Failed to create or fetch Logflare endpoint '{name}'") from exc


# --- BRANCH ENDPOINT ---
async def create_branch_endpoint(branch_id: str) -> str:
    """
    Creates a single endpoint aggregating all branch-specific sources into one query.
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

    description = f"Aggregated all logs endpoint for branch {branch_id}"
    return await _create_endpoint(endpoint_name, description, sql_query)


# --- GLOBAL ENDPOINT ---
async def create_global_endpoint() -> str:
    """
    Creates a global endpoint aggregating default Logflare sources (not tied to any branch).
    """
    endpoint_name = "global.logs.all"

    sql_query = """
    WITH auth_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `global.auth.logs.vela` AS t
    ),
    controller_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `global.controller.logs.vela` AS t
    ),
    studio_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `global.studio.logs.vela` AS t
    ),
    db_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `global.db.logs.vela` AS t
    ),
    kong_logs AS (
      SELECT t.timestamp, t.id, t.event_message, t.metadata
      FROM `global.kong.logs.vela` AS t
    )
    SELECT id, timestamp, event_message, metadata
    FROM auth_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM controller_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM studio_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM db_logs
    UNION ALL
    SELECT id, timestamp, event_message, metadata FROM kong_logs
    ORDER BY CAST(timestamp AS timestamp) DESC
    LIMIT 100;
    """

    description = "Aggregated global logs endpoint for system-wide visibility"
    return await _create_endpoint(endpoint_name, description, sql_query)


# --- LOG QUERY ---
async def get_logs_from_endpoint(branch_id: str, source: str, limit: int = 100):
    """
    Query logs for a specific source using the branch endpoint.
    Example source: 'postgres.logs' or 'realtime.logs.prod'
    Raises VelaLogflareError on failure.
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
            logger.info(f"Retrieved {len(data)} logs from '{source}' via endpoint '{endpoint_name}'.")
            return data

        except (httpx.HTTPError, ValueError) as exc:
            raise VelaLogflareError(f"Failed to fetch logs for '{source}' via endpoint '{endpoint_name}'") from exc


# --- SOURCE DELETION ---
async def delete_branch_sources(branch_id: str) -> None:
    """
    Delete all Logflare sources for a specific branch.
    """
    await _delete_sources(prefix=f"{branch_id}.")


# --- DELETE GLOBAL SOURCES ---
async def delete_global_sources() -> None:
    """
    Delete all global Logflare sources.
    """
    await _delete_sources(prefix="global.")


# --- SOURCE DELETION ---
async def _delete_sources(prefix: str) -> None:
    """
    Shared helper to delete all Logflare sources whose names start with the given prefix.
    Raises VelaLogflareError on failure.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            list_resp = await client.get(f"{settings.logflare_url}/api/sources", headers=headers)
            list_resp.raise_for_status()
            sources = list_resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise VelaLogflareError(f"Failed to list Logflare sources with prefix '{prefix}'") from exc

        filtered_sources = [s for s in sources if s.get("name", "").startswith(prefix)]
        if not filtered_sources:
            logger.info(f"No sources found with prefix '{prefix}'.")
            return

        for src in filtered_sources:
            src_id = src.get("id")
            src_name = src.get("name")

            if not src_id:
                logger.warning(f"Skipping source '{src_name}' without ID.")
                continue

            delete_url = f"{settings.logflare_url}/api/sources/{src_id}"
            try:
                del_resp = await client.delete(delete_url, headers=headers)
                del_resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise VelaLogflareError(f"Failed to delete Logflare source with prefix '{prefix}'") from exc


# --- ENDPOINT DELETION ---
async def delete_branch_endpoint(branch_id: str) -> None:
    """
    Delete the aggregated Logflare endpoint for a specific branch.
    """
    endpoint_name = f"{branch_id}.logs.all"
    await _delete_endpoint_by_name(endpoint_name)


# --- DELETE GLOBAL ENDPOINT ---
async def delete_global_endpoint() -> None:
    """
    Delete the global aggregated Logflare endpoint (no branch).
    """
    endpoint_name = "global.logs.all"
    await _delete_endpoint_by_name(endpoint_name)


async def _delete_endpoint_by_name(endpoint_name: str) -> None:
    """
    Shared helper to delete a Logflare endpoint by its name.
    Raises VelaLogflareError on failure.
    """
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            list_resp = await client.get(f"{settings.logflare_url}/api/endpoints", headers=headers)
            list_resp.raise_for_status()

        except (httpx.HTTPError, ValueError) as exc:
            raise VelaLogflareError("Failed to list Logflare endpoints") from exc

        endpoints = list_resp.json()

        endpoint = next((e for e in endpoints if e.get("name") == endpoint_name), None)
        if not endpoint:
            logger.info(f"No endpoint found with name '{endpoint_name}'.")
            return

        endpoint_id = endpoint.get("id")
        if not endpoint_id:
            logger.warning(f"Endpoint '{endpoint_name}' has no ID; skipping deletion.")
            return

        delete_url = f"{settings.logflare_url}/api/endpoints/{endpoint_id}"
        try:
            del_resp = await client.delete(delete_url, headers=headers)
            del_resp.raise_for_status()
            logger.info(f"Deleted Logflare endpoint '{endpoint_name}' (id={endpoint_id}).")

        except httpx.HTTPError as exc:
            raise VelaLogflareError(f"Failed to delete Logflare endpoint '{endpoint_name}'") from exc


async def create_branch_logflare_objects(branch_id: Identifier):
    """
    Creates a Logflare source and endpoint for a given branch.
    """
    logger.info(f"Creating Logflare objects for branch_id={branch_id}")

    sources = await create_branch_sources(str(branch_id))
    endpoint_id = await create_branch_endpoint(str(branch_id))

    logger.info(f"Created {len(sources)} sources and endpoint {endpoint_id} for branch '{branch_id}'.")


async def create_global_logflare_objects():
    """
    Creates global Logflare sources and endpoint (not tied to any branch).
    """
    logger.info("Creating global Logflare sources and endpoint...")

    sources = await create_global_sources()
    endpoint_id = await create_global_endpoint()

    logger.info(f"Created {len(sources)} global sources and endpoint {endpoint_id}.")

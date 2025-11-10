import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx
from httpx import Response

from .._util import Identifier
from ..exceptions import VelaLogflareError
from .settings import get_settings

logger = logging.getLogger(__name__)


async def _raise_for_status(response: Response) -> None:
    response.raise_for_status()


@asynccontextmanager
async def _client(timeout: int = 10) -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        base_url=f"{get_settings().logflare_url}/api/",
        timeout=timeout,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {get_settings().logflare_private_access_token}",
        },
        event_hooks={
            "response": [_raise_for_status],
        },
    ) as client:
        yield client


async def _create_sources(sources: list[str], prefix: str | None = None) -> list[str]:
    """
    Internal helper for creating Logflare sources.
    If a prefix (e.g., branch_id) is provided, it prefixes each source name.
    Returns a list of successfully created or existing source names.
    Raises VelaLogflareError on unrecoverable failures.
    """
    created_sources = []

    async with _client() as client:
        try:
            # Fetch existing sources once to avoid repeated calls
            existing_sources = {s["name"] for s in (await client.get("sources")).json()}

        except httpx.HTTPError as exc:
            raise VelaLogflareError("Failed to list existing Logflare sources") from exc

        for name in sources:
            full_name = f"{prefix}.{name}" if prefix else name

            # Skip creation if already exists
            if full_name in existing_sources:
                logger.info(f"Logflare source '{full_name}' already exists. Skipping creation.")
                created_sources.append(full_name)
                continue

            try:
                payload = {"name": full_name, "default_ingest_backend_enabled": True}
                source = await client.post("/sources", json=payload)

                # Chris: I hate this. I really hate this. You wouldn't believe how much I hate this.
                # But it forces the underlying source table to create. That's all that's required 🤷‍♂️
                payload = {"message": "Successfully created log source"}
                await client.post(f"/logs?source={source.json()['token']}", json=payload)

                logger.info(f"Logflare source '{full_name}' created successfully.")
                created_sources.append(full_name)
            except httpx.HTTPError as exc:
                raise VelaLogflareError(f"Failed to create Logflare source '{full_name}'") from exc

    return created_sources


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
    async with _client(timeout=60) as client:
        try:
            endpoints = (await client.get("/endpoints")).json()

            if (
                endpoint_id := next((endpoint["id"] for endpoint in endpoints if endpoint.get("name") == name), None)
            ) is not None:
                logger.info(f"Endpoint '{name}' already exists (id={endpoint_id}). Skipping creation.")
                return endpoint_id

            data = (
                await client.post(
                    "/endpoints",
                    json={
                        "name": name,
                        "description": description,
                        "enable_auth": True,
                        "sandboxable": True,
                        "query": sql_query.strip(),
                    },
                )
            ).json()
            endpoint_id = data.get("id") or data.get("endpoint_id")
            logger.info(f"Created Logflare endpoint '{name}' successfully (id={endpoint_id}).")
            return endpoint_id

        except httpx.HTTPError as exc:
            raise VelaLogflareError(f"Failed to create or fetch Logflare endpoint '{name}'") from exc


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

    async with _client(timeout=30) as client:
        try:
            data = (
                await client.get(
                    f"/endpoints/query/{endpoint_name}",
                    params={"pg_sql": pg_sql_query, "project": branch_id},
                )
            ).json()
            logger.info(f"Retrieved {len(data)} logs from '{source}' via endpoint '{endpoint_name}'.")
            return data

        except (httpx.HTTPError, ValueError) as exc:
            raise VelaLogflareError(f"Failed to fetch logs for '{source}' via endpoint '{endpoint_name}'") from exc


async def delete_branch_sources(branch_id: str) -> None:
    """
    Delete all Logflare sources for a specific branch.
    """
    await _delete_sources(prefix=f"{branch_id}.")


async def delete_global_sources() -> None:
    """
    Delete all global Logflare sources.
    """
    await _delete_sources(prefix="global.")


async def _delete_sources(prefix: str) -> None:
    """
    Shared helper to delete all Logflare sources whose names start with the given prefix.
    Raises VelaLogflareError on failure.
    """
    async with _client() as client:
        try:
            sources = (await client.get("/sources")).json()
        except (httpx.HTTPError, ValueError) as exc:
            raise VelaLogflareError(f"Failed to list Logflare sources with prefix '{prefix}'") from exc

        filtered_sources = [s for s in sources if s.get("name", "").startswith(prefix)]
        if not filtered_sources:
            logger.info(f"No sources found with prefix '{prefix}'.")
            return

        for src in filtered_sources:
            src_id = src.get("id")
            src_name = src.get("name")
            src_token = src.get("token")

            if not src_id:
                logger.warning(f"Skipping source '{src_name}' without ID.")
                continue

            try:
                await client.delete(f"/sources/{src_token}")
            except httpx.HTTPError as exc:
                raise VelaLogflareError(f"Failed to delete Logflare source with prefix '{prefix}'") from exc


async def delete_branch_endpoint(branch_id: str) -> None:
    """
    Delete the aggregated Logflare endpoint for a specific branch.
    """
    endpoint_name = f"{branch_id}.logs.all"
    await _delete_endpoint_by_name(endpoint_name)


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
    async with _client() as client:
        try:
            list_resp = await client.get("/endpoints")
            list_resp.raise_for_status()

        except (httpx.HTTPError, ValueError) as exc:
            raise VelaLogflareError("Failed to list Logflare endpoints") from exc

        endpoints = list_resp.json()

        endpoint = next((e for e in endpoints if e.get("name") == endpoint_name), None)
        if not endpoint:
            logger.info(f"No endpoint found with name '{endpoint_name}'.")
            return

        endpoint_id = endpoint.get("id")
        endpoint_token = endpoint.get("token")
        if not endpoint_id:
            logger.warning(f"Endpoint '{endpoint_name}' has no ID; skipping deletion.")
            return

        try:
            del_resp = await client.delete(f"/endpoints/{endpoint_token}")
            del_resp.raise_for_status()
            logger.info(f"Deleted Logflare endpoint '{endpoint_name}' (id={endpoint_id}) (token={endpoint_token}).")

        except httpx.HTTPError as exc:
            raise VelaLogflareError(f"Failed to delete Logflare endpoint '{endpoint_name}'") from exc


async def create_branch_logflare_objects(branch_id: Identifier):
    """
    Creates a Logflare source and endpoint for a given branch.
    """
    logger.info(f"Creating Logflare objects for branch_id={branch_id}")

    sources = await _create_sources(
        [
            "realtime.logs.prod",
            "postgREST.logs.prod",
            "postgres.logs",
            "deno-relay-logs",
            "storage.logs.prod.2",
        ],
        prefix=str(branch_id),
    )
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
    endpoint_id = await _create_endpoint(endpoint_name, description, sql_query)

    logger.info(f"Created {len(sources)} sources and endpoint {endpoint_id} for branch '{branch_id}'.")


async def create_global_logflare_objects():
    """
    Creates global Logflare sources and endpoint (not tied to any branch).
    """
    logger.info("Creating global Logflare sources and endpoint...")

    sources = await _create_sources(
        [
            "global.auth.logs.vela",
            "global.controller.logs.vela",
            "global.studio.logs.vela",
            "global.db.logs.vela",
            "global.kong.logs.vela",
        ]
    )
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
    endpoint_id = await _create_endpoint(endpoint_name, description, sql_query)

    logger.info(f"Created {len(sources)} global sources and endpoint {endpoint_id}.")


async def delete_branch_logflare_objects(branch_id: Identifier):
    """
    Delete all Logflare objects (sources and endpoint) associated with a specific branch.
    This function combines deletion of sources and the aggregated endpoint.
    """
    logger.info(f"Deleting all Logflare objects for branch_id={branch_id}")
    try:
        await delete_branch_sources(str(branch_id))
        logger.info(f"Deleted all sources for branch '{branch_id}' successfully.")
    except VelaLogflareError as exc:
        logger.error(f"Failed to delete sources for branch '{branch_id}': {exc}")
        raise

    try:
        await delete_branch_endpoint(str(branch_id))
        logger.info(f"Deleted endpoint for branch '{branch_id}' successfully.")
    except VelaLogflareError as exc:
        logger.error(f"Failed to delete endpoint for branch '{branch_id}': {exc}")
        raise

    logger.info(f"Successfully deleted all Logflare objects for branch '{branch_id}'.")

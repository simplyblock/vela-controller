import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import httpx

from .._util import Identifier
from ..exceptions import VelaGrafanaError
from .settings import get_settings

logger = logging.getLogger(__name__)


async def _raise_for_status(response: httpx.Response) -> None:
    response.raise_for_status()


@asynccontextmanager
async def _client(timeout: int = 10) -> AsyncGenerator[httpx.AsyncClient]:
    settings = get_settings()
    async with httpx.AsyncClient(
        base_url=f"{settings.grafana_url}/api/",
        timeout=timeout,
        headers={"Content-Type": "application/json"},
        auth=(settings.grafana_security_admin_user, settings.grafana_security_admin_password),
        event_hooks={
            "response": [_raise_for_status],
        },
    ) as client:
        yield client


async def create_vela_grafana_obj(organization_id: Identifier, branch_id: Identifier, credential):
    logger.info(f"Creating Grafana object for organization={organization_id}, branch={branch_id}")

    team_id = await create_team(str(branch_id))
    parent_folder_id = await create_folder(str(organization_id))

    await set_folder_permissions(parent_folder_id, team_id)
    folder_id = await create_folder(str(branch_id), parent_uid=parent_folder_id)
    await set_folder_permissions(folder_id, team_id)

    user_id = await get_user_via_jwt(credential)
    await add_user_to_team(team_id, user_id)
    await create_dashboard(str(organization_id), folder_id, str(branch_id))


async def delete_vela_grafana_obj(branch_id: Identifier):
    logger.info(f"Deleting Grafana objects branch={branch_id}")

    async with _client() as client:
        try:
            res = await client.get("folders")
            folders = res.json()
            branch_folder_uid = next((f["uid"] for f in folders if f["title"] == str(branch_id)), None)

            if branch_folder_uid:
                await remove_folder(branch_folder_uid)
            else:
                logger.warning(f"No folder found for branch '{branch_id}'")

            team_search = await client.get(f"teams/search?name={branch_id}")
            teams = team_search.json().get("teams", [])
            if teams:
                team_id = teams[0]["id"]
                await remove_team(team_id)
            else:
                logger.warning(f"No team found for branch '{branch_id}'")

            logger.info(f"Grafana objects deleted branch={branch_id}")

        except httpx.HTTPError as exc:
            logger.exception(f"HTTP error while deleting Grafana objects for branch '{branch_id}': {exc}")
            raise VelaGrafanaError(f"Failed to delete Grafana objects for branch '{branch_id}': {exc}") from exc

        except Exception as exc:
            logger.exception(f"Unexpected error while deleting Grafana objects for branch '{branch_id}': {exc}")
            raise VelaGrafanaError(f"Unexpected error deleting Grafana objects: {exc}") from exc


# --- TEAM CREATION ---
async def create_team(team_name: str):
    async with _client() as client:
        try:
            response = await client.post("teams", json={"name": team_name})
            logger.info(f"Team '{team_name}' created successfully.")
            return response.json().get("teamId")

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 409:
                logger.warning(f"Team '{team_name}' already exists. Fetching existing team ID...")
                try:
                    res = await client.get("search?name={team_name}")
                    team_id = res.json()["teams"][0]["id"]
                    logger.info(f"Fetched existing team ID: {team_id}")
                    return team_id
                except Exception as fetch_exc:
                    logger.error(f"Failed to fetch existing team '{team_name}': {fetch_exc}")
                    raise VelaGrafanaError(f"Failed to fetch existing team '{team_name}'") from fetch_exc

            logger.error(f"Failed to create Grafana team '{team_name}': {exc}")
            raise VelaGrafanaError(f"Failed to create Grafana team '{team_name}': {exc}") from exc


# --- FOLDER CREATION ---
async def create_folder(folder_name: str, parent_uid: str | None = None) -> str:
    async with _client() as client:
        try:
            payload = {"title": folder_name}
            if parent_uid:
                payload["parentUid"] = parent_uid

            response = await client.post("folders", json=payload)
            logger.info(f"Folder '{folder_name}' created successfully.")
            return response.json()["uid"]

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 412:
                logger.warning(f"Folder '{folder_name}' already exists. Fetching existing UID...")
                try:
                    res = await client.get("folders")
                    res.raise_for_status()
                    for f in res.json():
                        if f["title"] == folder_name:
                            logger.info(f"Fetched existing folder UID: {f['uid']}")
                            return f["uid"]
                except Exception as fetch_exc:
                    logger.error(f"Failed to fetch folder '{folder_name}': {fetch_exc}")
                    raise VelaGrafanaError(f"Failed to fetch existing folder '{folder_name}'") from fetch_exc

            logger.error(f"Failed to create Grafana folder '{folder_name}': {exc}")
            raise VelaGrafanaError(f"Failed to create Grafana folder '{folder_name}'") from exc


# --- PERMISSIONS ---
async def set_folder_permissions(folder_uid: str, team_id: int):
    async with httpx.AsyncClient() as client:
        try:
            payload = {"items": [{"teamId": team_id, "permission": 1}]}
            await client.post(f"folders/{folder_uid}/permissions", json=payload)
            logger.info(f"Permissions set for team {team_id} on folder {folder_uid}.")

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                logger.warning(f"Folder {folder_uid} not found while setting permissions.")
            elif status_code == 403:
                logger.warning(f"Insufficient permission to set folder permissions for {folder_uid}.")
            else:
                logger.error(f"HTTP error while setting permissions for {folder_uid}: {exc}")
            raise VelaGrafanaError(f"Failed to set folder permissions for {folder_uid}: {exc}") from exc

        except httpx.HTTPError as exc:
            logger.exception(f"Unexpected error setting folder permissions for {folder_uid}")
            raise VelaGrafanaError(f"Unexpected error setting folder permissions: {exc}") from exc


# --- USER MANAGEMENT ---
async def get_user_via_jwt(jwt_token: str):
    async with httpx.AsyncClient() as client:
        try:
            jwt_headers = {"Authorization": f"Bearer {jwt_token}"}
            response = await client.get(f"{get_settings().grafana_url}/api/user", headers=jwt_headers)
            response.raise_for_status()
            user_info = response.json()
            logger.info(f"Authenticated as '{user_info['login']}' ({user_info['email']})")
            return user_info["id"]

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 401:
                logger.warning("Invalid or expired JWT token.")
            elif status_code == 403:
                logger.warning("Access denied while authenticating user via JWT.")
            else:
                logger.error(f"HTTP error during JWT authentication: {exc}")
            raise VelaGrafanaError(f"Failed to authenticate via JWT: {exc}") from exc

        except httpx.HTTPError as exc:
            logger.exception("Unexpected error during JWT authentication.")
            raise VelaGrafanaError(f"Unexpected error authenticating user: {exc}") from exc


async def add_user_to_team(team_id: int, user_id: int):
    async with _client() as client:
        try:
            response = await client.post(f"teams/{team_id}/members", json={"userId": user_id})
            if response.status_code == 200:
                logger.info(f"User {user_id} added to team {team_id}.")
            elif response.status_code == 400:
                logger.warning(f"User {user_id} already in team {team_id}.")
            else:
                response.raise_for_status()

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 404:
                logger.warning(f"Team {team_id} not found when adding user {user_id}.")
            else:
                logger.error(f"HTTP error adding user {user_id} to team {team_id}: {exc}")
            raise VelaGrafanaError(f"Failed to add user to team: {exc}") from exc

        except httpx.HTTPError as exc:
            logger.exception(f"Unexpected error adding user {user_id} to team {team_id}.")
            raise VelaGrafanaError(f"Unexpected error adding user to team: {exc}") from exc


async def remove_team(team_id: int):
    async with _client() as client:
        try:
            await client.delete(f"teams/{team_id}")
            logger.info(f"Team {team_id} removed.")

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 403:
                raise VelaGrafanaError(f"Permission denied when removing team {team_id}.") from exc
            elif status_code == 404:
                logger.warning(f"Team {team_id} not found.")
            else:
                raise VelaGrafanaError(f"HTTP error removing team {team_id}: {exc}") from exc

        except httpx.HTTPError as exc:
            logger.exception(f"Unexpected error removing team {team_id}.")
            raise VelaGrafanaError(f"Unexpected error removing team: {exc}") from exc


async def remove_folder(folder_uid: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.delete(f"folders/{folder_uid}")
            logger.info(f"Folder {folder_uid} removed.")

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 403:
                raise VelaGrafanaError(f"Permission denied when removing folder {folder_uid}.") from exc
            elif status_code == 404:
                logger.warning(f"Folder {folder_uid} not found.")
            else:
                raise VelaGrafanaError(f"HTTP error removing folder {folder_uid}: {exc}") from exc

        except httpx.HTTPError as exc:
            raise VelaGrafanaError(f"Unexpected error removing folder: {exc}") from exc


async def remove_user_from_team(team_id: int, user_id: int):
    async with httpx.AsyncClient() as client:
        try:
            await client.delete(f"teams/{team_id}/members/{user_id}")
            logger.info(f"User {user_id} removed from team {team_id}.")

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code == 403:
                raise VelaGrafanaError(f"Permission denied when removing user {user_id} from team {team_id}.") from exc
            elif status_code == 404:
                logger.warning(f"User {user_id} not found in team {team_id}.")
            else:
                raise VelaGrafanaError(f"HTTP error removing user {user_id} from team {team_id}: {exc}") from exc

        except httpx.HTTPError as exc:
            logger.exception(f"Unexpected error removing user {user_id} from team {team_id}.")
            raise VelaGrafanaError(f"Unexpected error removing user from team: {exc}") from exc


# --- DASHBOARD CREATION ---
async def create_dashboard(org_name: str, folder_uid: str, folder_name: str):
    dashboard_payload = {
        "dashboard": {
            "id": None,
            "uid": None,
            "title": f"{folder_name} Metrics",
            "tags": [folder_name],
            "timezone": "browser",
            "schemaVersion": 36,
            "version": 0,
            "panels": [
                {
                    "type": "timeseries",
                    "title": "Example Metric",
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0},
                    "datasource": {"type": "prometheus", "uid": "eev2sidbr5ekgb"},
                    "targets": [
                        {
                            "expr": 'custom_metric_value{org="$organization",proj="$project"}',
                            "legendFormat": "{{instance}}",
                            "refId": "A",
                        }
                    ],
                }
            ],
            "templating": {
                "list": [
                    {
                        "name": "organization",
                        "type": "constant",
                        "label": org_name,
                        "query": org_name,
                        "current": {"selected": True, "text": org_name, "value": org_name},
                    },
                    {
                        "name": "project",
                        "type": "constant",
                        "label": folder_name,
                        "query": folder_name,
                        "current": {"selected": True, "text": folder_name, "value": folder_name},
                    },
                ]
            },
        },
        "folderUid": folder_uid,
        "overwrite": True,
    }

    async with _client() as client:
        try:
            await client.post("dashboards/db", json=dashboard_payload)
            logger.info(f"Dashboard created successfully in folder '{folder_name}'.")
        except httpx.HTTPError as exc:
            logger.error(f"Failed to create dashboard for folder '{folder_name}': {exc}")
            raise VelaGrafanaError(f"Failed to create dashboard: {exc}") from exc

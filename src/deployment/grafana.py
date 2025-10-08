import os
import requests
import json

NAMESPACE = os.environ.get("VELA_DEPLOYMENT_NAMESPACE_PREFIX", "")

GRAFANA_URL = f"http://vela-grafana.{NAMESPACE}.svc.cluster.local:3000"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "password"

# Basic Auth for Grafana
auth = (GRAFANA_USER, GRAFANA_PASSWORD)
headers = {"Content-Type": "application/json"}


# Create Team
def create_team(team_name):
    payload = {"name": team_name}
    r = requests.post(f"{GRAFANA_URL}/api/teams", auth=auth, headers=headers, data=json.dumps(payload))
    if r.status_code == 200:
        print(f"[+] Team '{team_name}' created.")
        return r.json().get("teamId")
    elif r.status_code == 409:
        print(f"[!] Team '{team_name}' already exists. Fetching ID...")
        # Fetch existing team
        res = requests.get(f"{GRAFANA_URL}/api/teams/search?name={team_name}", auth=auth)
        return res.json()["teams"][0]["id"]
    else:
        raise Exception(f"Failed to create team: {r.text}")

# Create Folder
def create_folder(folder_name, parent_uid=None):
    payload = {"title": folder_name}
    if parent_uid:
        payload["parentUid"] = parent_uid
    r = requests.post(f"{GRAFANA_URL}/api/folders", auth=auth, headers=headers, data=json.dumps(payload))
    if r.status_code == 200:
        print(f"[+] Folder '{folder_name}' created.")
        return r.json()["uid"]
    elif r.status_code == 412:
        print(f"[!] Folder '{folder_name}' already exists. Fetching ID...")
        # Fetch existing folder
        res = requests.get(f"{GRAFANA_URL}/api/folders", auth=auth)
        for f in res.json():
            if f["title"] == folder_name:
                return f["uid"]
    else:
        raise Exception(f"Failed to create folder: {r.text}")

# Link Team to Folder Permissions
def set_folder_permissions(folder_uid, team_id):
    permissions_payload = {
        "items": [
            {
                "teamId": team_id,
                "permission": 1  # 1=View, 2=Edit, 4=Admin
            }
        ]
    }

    r = requests.post(f"{GRAFANA_URL}/api/folders/{folder_uid}/permissions",
                      auth=auth, headers=headers, data=json.dumps(permissions_payload))
    if r.status_code == 200:
        print(f"[+] Permissions set for team {team_id} on folder {folder_uid}.")
    else:
        raise Exception(f"Failed to set folder permissions: {r.text}")



def get_user_via_jwt(GRAFANA_JWT):
    jwt_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {GRAFANA_JWT}"
    }
    r = requests.get(f"{GRAFANA_URL}/api/user", headers=jwt_headers)
    if r.status_code == 200:
        user_info = r.json()
        print(f"[+] Authenticated as '{user_info['login']}' ({user_info['email']})")
        return user_info["id"]
    else:
        raise Exception(f"Failed to authenticate via JWT: {r.status_code} {r.text}")

# Add User to Team
def add_user_to_team(team_id, user_id):
    payload = {"userId": user_id}
    r = requests.post(f"{GRAFANA_URL}/api/teams/{team_id}/members", auth=auth, headers=headers, data=json.dumps(payload))
    if r.status_code == 200:
        print(f"[+] User {user_id} added to team {team_id}.")
    elif r.status_code == 400:
        print(f"[!] User {user_id} is already in team {team_id}.")
    else:
        raise Exception(f"Failed to add user to team: {r.text}")

def remove_team(team_id):
    r = requests.delete(f"{GRAFANA_URL}/api/teams/{team_id}", auth=auth, headers=headers)
    if r.status_code == 200:
        print(f"[+] Team {team_id} removed.")
    elif r.status_code == 404:
        print(f"[!] Team {team_id} not found.")
    else:
        raise Exception(f"Failed to remove team: {r.text}")


# Remove Folder
def remove_folder(folder_uid):
    r = requests.delete(f"{GRAFANA_URL}/api/folders/{folder_uid}", auth=auth, headers=headers)
    if r.status_code == 200:
        print(f"[+] Folder {folder_uid} removed.")
    elif r.status_code == 404:
        print(f"[!] Folder {folder_uid} not found.")
        
    else:
        raise Exception(f"Failed to remove folder: {r.text}")

def remove_user_from_team(team_id, user_id):
    r = requests.delete(
        f"{GRAFANA_URL}/api/teams/{team_id}/members/{user_id}",
        auth=auth,
        headers=headers
    )
    if r.status_code == 200:
        print(f"[+] User {user_id} removed from team {team_id}.")
    elif r.status_code == 404:
        print(f"[!] User {user_id} not found in team {team_id}.")
    else:
        raise Exception(f"Failed to remove user from team: {r.text}")

def create_dashboard(org_name, folder_uid, folder_name):
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
                            "expr": f'custom_metric_value{{org="$organization",proj="$project"}}',
                            "legendFormat": "{{instance}}",
                            "refId": "A"
                        }
                    ]
                }
            ],
            "templating": {
                "list": [
                    {
                        "name": "organization",
                        "type": "constant",
                        "label": org_name,
                        "query": org_name,
                        "current": {
                            "selected": True,
                            "text": org_name,
                            "value": org_name
                        }
                    },

                    {
                        "name": "project",
                        "type": "constant",
                        "label": folder_name,
                        "query": folder_name,
                        "current": {
                            "selected": True,
                            "text": folder_name,
                            "value": folder_name
                        }
                    }
                ]
            }
        },
        "folderUid": folder_uid,
        "overwrite": True
    }

    r = requests.post(f"{GRAFANA_URL}/api/dashboards/db",
                      auth=auth, headers=headers, data=json.dumps(dashboard_payload))

    if r.status_code in (200, 202):
        print(f"[+] Dashboard created in folder '{folder_name}' with project variable.")
    else:
        raise Exception(f"Failed to create dashboard: {r.text}")


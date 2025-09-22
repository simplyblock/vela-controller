import logging
import subprocess
import tempfile
from importlib import resources
from typing import Annotated, Literal

import yaml
from kubernetes.client.rest import ApiException
from pydantic import BaseModel, Field
from urllib3.exceptions import HTTPError

from .._util import check_output, dbstr
from .kubernetes import KubernetesService
from .settings import settings

logger = logging.getLogger(__name__)

kube_service = KubernetesService()


def _deployment_namespace(id_: int) -> str:
    return f"{settings.deployment_namespace_prefix}-deployment-{id_}"


def _release_name(namespace: str) -> str:
    return f"supabase-{namespace}"


def _branch_release_name(namespace: str, branch_id: int) -> str:
    return f"supabase-{namespace}-b-{branch_id}"


def _db_pvc_name_for_release(release_name: str) -> str:
    # Matches include "supabase.db.fullname" + "-pvc"; fullname resolves to f"{Release.Name}-supabase-db"
    return f"{release_name}-supabase-db-pvc"


class DeploymentParameters(BaseModel):
    database: dbstr
    database_user: dbstr
    database_password: dbstr
    database_size: Annotated[int, Field(gt=0, multiple_of=2**30)]
    vcpu: int
    memory: Annotated[int, Field(gt=0, multiple_of=2**30)]
    iops: int
    database_image_tag: Literal["15.1.0.147"]


StatusType = Literal["ACTIVE_HEALTHY", "ACTIVE_UNHEALTHY", "COMING_UP", "INACTIVE", "UNKNOWN"]


class DeploymentStatus(BaseModel):
    status: StatusType
    pods: dict[str, str]
    message: str


async def create_vela_config(id_: int, parameters: DeploymentParameters):
    logging.info(
        f"Creating Vela configuration for namespace: {_deployment_namespace(id_)}"
        f" (database {parameters.database}, user {parameters.database_user})"
    )

    chart = resources.files(__package__) / "charts" / "supabase"
    values_content = yaml.safe_load((chart / "values.example.yaml").read_text())

    # Override defaults
    db_secrets = values_content.setdefault("secret", {}).setdefault("db", {})
    db_secrets["username"] = parameters.database_user
    db_secrets["password"] = parameters.database_password
    db_secrets["database"] = parameters.database

    db_spec = values_content.setdefault("db", {})
    db_spec["vcpu"] = parameters.vcpu
    db_spec["ram"] = parameters.memory // (2**30)
    db_spec.setdefault("persistence", {})["size"] = f"{parameters.database_size // (2**30)}Gi"
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag

    values_content["kong"]["ingress"]["hosts"][0]["host"] = settings.deployment_host
    values_content["kong"]["ingress"]["hosts"][0]["paths"][0]["path"] = f"/{id_}"

    namespace = _deployment_namespace(id_)

    # todo: create an storage class with the given IOPS
    values_content["provisioning"] = {"iops": parameters.iops}
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)

        try:
            await check_output(
                [
                    "helm",
                    "install",
                    _release_name(namespace),
                    str(chart),
                    "--namespace",
                    namespace,
                    "--create-namespace",
                    "-f",
                    temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create deployment: {e.stderr}")
            await check_output(
                ["helm", "uninstall", f"supabase-{namespace}", "-n", namespace],
                stderr=subprocess.PIPE,
                text=True,
            )
            raise


async def create_branch_deployment(
    project_id: int,
    parameters: DeploymentParameters,
    branch_id: int,
    *,
    clone_from_branch_id: int | None,
    data_copy: bool,
):
    """
    Create a deployment for a project branch. If data_copy is True, pre-create a cloned PVC.
    The branch deployment is installed as a separate Helm release in the same namespace
    as the project, to allow PVC cloning within the namespace.
    """
    namespace = _deployment_namespace(project_id)
    release_name = _branch_release_name(namespace, branch_id)

    # Prepare values override similar to project with minor changes
    chart = resources.files(__package__) / "charts" / "supabase"
    values_content = yaml.safe_load((chart / "values.example.yaml").read_text())

    # Secrets / DB config inherited from project
    db_secrets = values_content.setdefault("secret", {}).setdefault("db", {})
    db_secrets["username"] = parameters.database_user
    db_secrets["password"] = parameters.database_password
    db_secrets["database"] = parameters.database

    db_spec = values_content.setdefault("db", {})
    db_spec["vcpu"] = parameters.vcpu
    db_spec["ram"] = parameters.memory // (2**30)
    db_spec.setdefault("image", {})["tag"] = parameters.database_image_tag

    # We pre-create PVC if cloning, so avoid Helm trying to create it again
    db_spec.setdefault("persistence", {})["enabled"] = not data_copy
    if not data_copy:
        db_spec.setdefault("persistence", {})["size"] = f"{parameters.database_size // (2**30)}Gi"

    values_content["kong"]["ingress"]["hosts"][0]["host"] = settings.deployment_host
    values_content["kong"]["ingress"]["hosts"][0]["paths"][0]["path"] = f"/{project_id}/branches/{branch_id}"

    # Optionally clone data PVC
    if data_copy:
        # Source release: main project or another branch
        if clone_from_branch_id is None:
            source_release = _release_name(namespace)
        else:
            source_release = _branch_release_name(namespace, clone_from_branch_id)
        src_pvc_name = _db_pvc_name_for_release(source_release)
        dst_pvc_name = _db_pvc_name_for_release(release_name)

        # Read source PVC and create a clone PVC spec
        v1 = kube_service.core_v1
        src_pvc = v1.read_namespaced_persistent_volume_claim(name=src_pvc_name, namespace=namespace)

        pvc_manifest = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {"name": dst_pvc_name},
            "spec": {
                # Clone retains same storage class, access modes and size
                "storageClassName": src_pvc.spec.storage_class_name,
                "accessModes": [m for m in (src_pvc.spec.access_modes or [])],
                "resources": {
                    "requests": {
                        "storage": src_pvc.spec.resources.requests.get("storage"),
                    }
                },
                "dataSource": {
                    "kind": "PersistentVolumeClaim",
                    "name": src_pvc_name,
                },
            },
        }

        logger.info(f"Creating cloned PVC {dst_pvc_name} from {src_pvc_name} in ns {namespace}")
        v1.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc_manifest)

    # Install Helm release for the branch
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)
        try:
            await check_output(
                [
                    "helm",
                    "install",
                    release_name,
                    str(chart),
                    "--namespace",
                    namespace,
                    # project namespace already exists; do not create explicitly here
                    "-f",
                    temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f"Failed to create branch deployment: {e.stderr}")
            # Best-effort cleanup
            try:
                await check_output(["helm", "uninstall", release_name, "-n", namespace], stderr=subprocess.PIPE, text=True)
            except Exception:  # noqa: BLE001
                pass
            raise


def _pods_with_status(statuses: dict[str, str], target_status: str) -> set[str]:
    return {name for name, status in statuses.items() if status == target_status}


def get_deployment_status(id_: int) -> DeploymentStatus:
    status: StatusType

    try:
        k8s_statuses = kube_service.check_namespace_status(_deployment_namespace(id_))

        if failed := _pods_with_status(k8s_statuses, "Failed"):
            status = "ACTIVE_UNHEALTHY"
            message = "Deployment has failed pods: " + ", ".join(failed)
        elif pending := _pods_with_status(k8s_statuses, "Pending"):
            status = "COMING_UP"
            message = "Deployment has pending pods: " + ", ".join(pending)
        elif succeeded := _pods_with_status(k8s_statuses, "Succeeded"):
            # succeeded implies a container is stopped, they should be running
            status = "INACTIVE"
            message = "Deployment has stopped pods: " + ", ".join(succeeded)
        elif all(status == "Running" for status in k8s_statuses.values()):
            status = "ACTIVE_HEALTHY"
            message = "All good :)"
        else:
            raise RuntimeError(
                "Unexpected status reported by kubernetes: "
                + "\n".join(f"{key}: {value}" for key, value in k8s_statuses.items())
            )

    except (ApiException, HTTPError, KeyError) as e:
        k8s_statuses = {}
        status = "UNKNOWN"
        message = str(e)

    return DeploymentStatus(
        status=status,
        pods=k8s_statuses,
        message=message,
    )


def delete_deployment(id_: int):
    namespace = _deployment_namespace(id_)
    subprocess.check_call(["helm", "uninstall", _release_name(namespace), "-n", namespace, "--wait"])
    kube_service.delete_namespace(namespace)


def get_db_vmi_identity(id_: int) -> tuple[str, str]:
    """
    Return the (namespace, vmi_name) for the project's database VirtualMachineInstance.

    The Helm chart defines the DB VM fullname as "{Release.Name}-{ChartName}-db" when no overrides
    are provided. Our release name is "supabase-{namespace}" and chart name is "supabase".
    Hence the VMI name resolves to: f"{_release_name(namespace)}-supabase-db".
    """
    namespace = _deployment_namespace(id_)
    vmi_name = f"{_release_name(namespace)}-supabase-db"
    return namespace, vmi_name

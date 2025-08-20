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

logger = logging.getLogger(__name__)

kube_service = KubernetesService()


def _deployment_namespace(id_: int) -> str:
    return f'vela-deployment-{id_}'


def _release_name(namespace: str) -> str:
    return f'supabase-{namespace}'


class DeploymentParameters(BaseModel):
    database: dbstr
    database_user: dbstr
    database_password: dbstr
    database_size: Annotated[int, Field(gt=0, multiple_of=2 ** 30)]
    vcpu: int
    memory: Annotated[int, Field(gt=0, multiple_of=2 ** 30)]
    iops: int
    database_image_tag: Literal['15.1.0.147']


StatusType = Literal['ACTIVE_HEALTHY', 'ACTIVE_UNHEALTHY', 'COMING_UP', 'INACTIVE', 'UNKNOWN']


class DeploymentStatus(BaseModel):
    status: StatusType
    pods: dict[str, str]
    message: str


async def create_vela_config(id_: int, parameters: DeploymentParameters):
    logging.info(f'Creating Vela configuration for namespace: {_deployment_namespace(id_)}'
        f' (database {parameters.database}, user {parameters.database_user})')

    chart = resources.files(__package__) / 'charts' / 'supabase'
    values_content = yaml.safe_load((chart / 'values.example.yaml').read_text())

    # Override defaults
    db_spec = values_content.setdefault('db', {})
    db_spec['username'] = parameters.database_user
    db_spec['database'] = parameters.database
    db_spec['password'] = parameters.database_password
    db_spec['vcpu'] = parameters.vcpu
    db_spec['ram'] = parameters.memory // (2 ** 30)
    db_spec.setdefault('persistence', {})['size'] = f'{parameters.database_size // (2 ** 30)}Gi'
    db_spec.setdefault('image', {})['tag'] = parameters.database_image_tag

    values_content['kong']['ingress']['hosts'][0]['paths'][0]['path'] = f'/{id_}'

    namespace = _deployment_namespace(id_)

    # todo: create an storage class with the given IOPS
    values_content['provisioning'] = {'iops': parameters.iops}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)

        try:
            await check_output(
                [
                    'helm', 'install', _release_name(namespace), str(chart),
                    '--namespace', namespace,
                    '--create-namespace',
                    '-f', temp_values.name,
                ],
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as e:
            logger.exception(f'Failed to create deployment: {e.stderr}')
            await check_output(
                    ['helm', 'uninstall', f'supabase-{namespace}', '-n', namespace],
                    stderr=subprocess.PIPE,
                    text=True,
            )
            raise


def _pods_with_status(statuses: dict[str, str], target_status: str) -> set[str]:
    return {
            name
            for name, status
            in statuses.items()
            if status == target_status
    }


def get_deployment_status(id_: int) -> DeploymentStatus:
    status: StatusType

    try:
        k8s_statuses = kube_service.check_namespace_status(_deployment_namespace(id_))

        if (failed := _pods_with_status(k8s_statuses, 'Failed')):
            status = 'ACTIVE_UNHEALTHY'
            message = 'Deployment has failed pods: ' + ', '.join(failed)
        elif (pending := _pods_with_status(k8s_statuses, 'Pending')):
            status = 'COMING_UP'
            message = 'Deployment has pending pods: ' + ', '.join(pending)
        elif (succeeded := _pods_with_status(k8s_statuses, 'Succeeded')):
            # succeeded implies a container is stopped, they should be running
            status = 'INACTIVE'
            message = 'Deployment has stopped pods: ' + ', '.join(succeeded)
        elif all(status == 'Running' for status in k8s_statuses.values()):
            status = 'ACTIVE_HEALTHY'
            message = 'All good :)'
        else:
            raise RuntimeError('Unexpected status reported by kubernetes: ' + '\n'.join(
                f'{key}: {value}'
                for key, value
                in k8s_statuses.items()
            ))

    except (ApiException, HTTPError, KeyError) as e:
        k8s_statuses = {}
        status = 'UNKNOWN'
        message = str(e)

    return DeploymentStatus(
        status=status,
        pods=k8s_statuses,
        message=message,
    )


def delete_deployment(id_: int):
    namespace = _deployment_namespace(id_)
    subprocess.check_call(['helm', 'uninstall', _release_name(namespace), '-n', namespace, '--wait'])
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


class ResizeParameters(BaseModel):
    vcpu: int | None = None
    memory: Annotated[int | None, Field(gt=0, multiple_of=2 ** 30)] = None
    database_size: Annotated[int | None, Field(gt=0, multiple_of=2 ** 30)] = None


class ResizeStatus(BaseModel):
    namespace: str
    vm_name: str
    vm_cpu_cores: int | None = None
    vm_memory_guest: str | None = None
    pvc_name: str
    pvc_phase: str | None = None
    pvc_capacity: str | None = None


def resize_deployment(id_: int, parameters: ResizeParameters):
    """Perform an in-place Helm upgrade to resize CPU, memory, and/or disk.

    Only parameters provided will be updated; others are preserved using --reuse-values.
    """
    chart = resources.files(__package__) / 'charts' / 'supabase'
    # Minimal values file with only overrides
    values_content: dict = {}
    db_spec = values_content.setdefault('db', {})
    if parameters.vcpu is not None:
        db_spec['vcpu'] = parameters.vcpu
    if parameters.memory is not None:
        db_spec['ram'] = parameters.memory // (2 ** 30)
    if parameters.database_size is not None:
        db_spec.setdefault('persistence', {})['size'] = f'{parameters.database_size // (2 ** 30)}Gi'

    namespace = _deployment_namespace(id_)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)

        subprocess.check_call([
            'helm', 'upgrade', _release_name(namespace), str(chart),
            '--namespace', namespace,
            '--reuse-values',
            '-f', temp_values.name,
        ])


def get_resize_status(id_: int) -> ResizeStatus:
    """Query KubeVirt VM and bound PVC for current status and capacity."""
    namespace, vm_name = get_db_vmi_identity(id_)
    # VM from KubeVirt
    vm_obj = None
    try:
        vm_obj = kube_service.custom.get_namespaced_custom_object(
            group='kubevirt.io', version='v1', namespace=namespace,
            plural='virtualmachines', name=vm_name,
        )
    except Exception:
        vm_obj = None

    # PVC from CoreV1
    pvc_name = f'{vm_name}-pvc'
    pvc_obj = None
    try:
        pvc_obj = kube_service.core_v1.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
    except Exception:
        pvc_obj = None

    vm_cpu = None
    vm_mem = None
    if isinstance(vm_obj, dict):
        domain = (((vm_obj or {}).get('spec') or {}).get('template') or {}).get('spec') or {}
        d_dom = (domain.get('domain') or {})
        cpu = (d_dom.get('cpu') or {})
        mem = (d_dom.get('memory') or {})
        vm_cpu = cpu.get('cores')
        vm_mem = mem.get('guest')

    pvc_phase = None
    pvc_capacity = None
    if pvc_obj is not None:
        try:
            pvc_phase = pvc_obj.status.phase
            pvc_capacity = (pvc_obj.status.capacity or {}).get('storage')
        except Exception:
            pass

    return ResizeStatus(
        namespace=namespace,
        vm_name=vm_name,
        vm_cpu_cores=vm_cpu,
        vm_memory_guest=vm_mem,
        pvc_name=pvc_name,
        pvc_phase=pvc_phase,
        pvc_capacity=pvc_capacity,
    )

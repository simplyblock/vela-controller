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

import logging
import subprocess
import tempfile
from importlib import resources
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field

from .._util import dbstr
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


class DeploymentStatus(BaseModel):
    status: str
    pods: list
    message: str


class DeleteDeploymentResponse(BaseModel):
    status: str
    deployment_id: str
    helm_output: str


def create_vela_config(id_: int, parameters: DeploymentParameters):
    logging.info(f'Creating Vela configuration for namespace: {_deployment_namespace(id_)}'
        ' (database {parameters.database}, user {parameters.database_user})')

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

    namespace = _deployment_namespace(id_)

    # todo: create an storage class with the given IOPS
    values_content['provisioning'] = {'iops': parameters.iops}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)

        try:
            subprocess.check_call([
                'helm', 'install', _release_name(namespace), str(chart),
                '--namespace', namespace,
                '--create-namespace',
                '-f', temp_values.name,
            ])
        except subprocess.CalledProcessError:
            subprocess.check_call([
                'helm', 'uninstall', f'supabase-{namespace}', '-n', namespace,
            ])
            raise


def get_deployment_status(id_: int) -> DeploymentStatus:
    k8s_status = kube_service.check_namespace_status(_deployment_namespace(id_))
    return DeploymentStatus(
        status=k8s_status['status'],
        pods=k8s_status['pods'],
        message=k8s_status['message'],
    )


def delete_deployment(id_: int):
    namespace = _deployment_namespace(id_)
    subprocess.check_call(['helm', 'uninstall', _release_name(namespace), '-n', namespace, '--wait'])
    kube_service.delete_namespace(namespace)

import os
import subprocess
import tempfile
from datetime import datetime
from importlib import resources
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, Field

from .kubernetes import KubernetesService

kube_service = KubernetesService()

class Deployment(BaseModel):
    namespace: str
    release_name: str
    database_user: str
    database_name: str
    status: Literal['pending', 'deploying', 'running', 'failed'] = 'pending'
    created_at: datetime = Field(default_factory=datetime.utcnow)


class DeploymentParameters(BaseModel):
    namespace: str
    database: str
    database_user: str
    database_password: str
    vcpu: int
    memory: Annotated[int, Field(multiple_of=2 ** 30)]
    database_size: Annotated[int, Field(multiple_of=2 ** 30)]
    iops: int
    database_image_tag: Literal['15.1.0.147']


class DeploymentStatus(BaseModel):
    namespace: str
    status: str
    pods: list
    message: str


class DeleteDeploymentResponse(BaseModel):
    status: str
    deployment_id: str
    helm_output: str


def create_vela_config(parameters: DeploymentParameters):
    os.environ['SECRET_DB_DATABASE'] = parameters.database
    os.environ['SECRET_DB_USERNAME'] = parameters.database_user
    os.environ['SECRET_DB_PASSWORD'] = parameters.database_password
    print(f'Creating Vela configuration for namespace: {parameters.namespace}')
    print(f'Database user: {parameters.database_user}')
    print(f'Database name: {parameters.database}')
    print('Database password: [REDACTED]')

    chart = resources.files(__package__) / 'charts' / 'supabase'
    values_content = yaml.safe_load((chart / 'values.example.yaml').read_text())

    # Override defaults
    db_spec = values_content.setdefault('db', {})
    db_spec['username'] = parameters.database_user
    db_spec['database'] = parameters.database
    db_spec['password'] = parameters.database_password
    db_spec['vcpu'] = parameters.vcpu
    db_spec['ram'] = parameters.memory // (2 ** 30)
    db_spec['persistence']['size'] = f'{parameters.database_size // (2 ** 30)}Gi'
    db_spec['image']['tag'] = parameters.database_image_tag

    # todo: create an storage class with the given IOPS
    values_content['provisioning'] = {'iops': parameters.iops}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_values:
        yaml.dump(values_content, temp_values, default_flow_style=False)

        try:
            release_name = f'supabase-{parameters.namespace}'
            subprocess.check_call([
                'helm', 'install', release_name, str(chart),
                '--namespace', parameters.namespace,
                '--create-namespace',
                '-f', temp_values.name,
            ])
            return Deployment(
                namespace=parameters.namespace,
                release_name=release_name,
                database_user=parameters.database_user,
                database_name=parameters.database,
                status='deploying',
            )
        except subprocess.CalledProcessError:
            subprocess.check_call([
                'helm', 'uninstall', f'supabase-{parameters.namespace}', '-n', parameters.namespace,
            ])
            raise


def get_deployment_status(deployment: Deployment):
    ns = deployment.namespace
    k8s_status = kube_service.check_namespace_status(ns)
    deployment.status = k8s_status['status']
    return DeploymentStatus(
        namespace=ns,
        status=k8s_status['status'],
        pods=k8s_status['pods'],
        message=k8s_status['message'],
    )


def delete_deployment(deployment: Deployment):
    subprocess.check_call(['helm', 'uninstall', deployment.release_name, '-n', deployment.namespace])

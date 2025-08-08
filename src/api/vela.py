from sqlmodel import Session, select
from typing import List
from ..db import SessionDep
from ..models.deployment import Deployment
from ..models.vela import (
    VelaCreateRequest, ErrorResponse, DeploymentCreateResponse, DeploymentItem, DeploymentStatusResponse, DeleteDeploymentResponse
)
from ..services.kubernetes_service import KubernetesService

kube_service = KubernetesService()

from sqlmodel import Session, select
from typing import List
from ..db import SessionDep
from ..models.deployment import Deployment
from ..models.vela import (
    VelaCreateRequest, ErrorResponse, DeploymentCreateResponse, DeploymentItem, DeploymentStatusResponse, DeleteDeploymentResponse
)
from ..services.kubernetes_service import KubernetesService

async def create_vela_config(request: VelaCreateRequest, session: Session):
    import os
    import subprocess
    import tempfile
    import yaml
    from pathlib import Path
    try:
        os.environ["SECRET_DB_USERNAME"] = request.dbuser
        os.environ["SECRET_DB_DATABASE"] = request.dbname
        os.environ["SECRET_DB_PASSWORD"] = request.dbpassword
        print(f"Creating Vela configuration for namespace: {request.namespace}")
        print(f"Database user: {request.dbuser}")
        print(f"Database name: {request.dbname}")
        print("Database password: [REDACTED]")
        project_root = Path(__file__).parent.parent.parent
        chart_path = project_root / "charts" / "supabase"
        values_file = project_root / "charts" / "supabase" / "values.example.yaml"
        if not chart_path.exists():
            raise HTTPException(status_code=404, detail=f"Supabase chart not found at {chart_path}")
        if not values_file.exists():
            raise HTTPException(status_code=404, detail=f"Values file not found at {values_file}")
        with open(values_file, 'r') as f:
            values_content = yaml.safe_load(f)
        if 'db' not in values_content:
            values_content['db'] = {}
        values_content['db']['username'] = request.dbuser
        values_content['db']['database'] = request.dbname
        values_content['db']['password'] = request.dbpassword
        values_content['provisioning'] = {
            'vcpu': request.vcpu,
            'ram': request.ram,
            'db_storage': request.db_storage,
            'iops': request.iops
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_values:
            yaml.dump(values_content, temp_values, default_flow_style=False)
            temp_values_path = temp_values.name
        try:
            release_name = f"supabase-{request.namespace}"
            helm_cmd = [
                "helm", "install", release_name, str(chart_path),
                "--namespace", request.namespace,
                "--create-namespace",
                "-f", temp_values_path
            ]
            subprocess.run(helm_cmd, check=True, capture_output=True, text=True)
            deployment = Deployment(
                namespace=request.namespace,
                release_name=release_name,
                db_user=request.dbuser,
                db_name=request.dbname,
                status="deploying"
            )
            session.add(deployment)
            session.commit()
            session.refresh(deployment)
            return DeploymentCreateResponse(deployment_id=str(deployment.id))
        except subprocess.CalledProcessError as e:
            cleanup_cmd = [
                "helm", "uninstall", f"supabase-{request.namespace}", "-n", request.namespace
            ]
            subprocess.run(cleanup_cmd, capture_output=True, text=True)
            return ErrorResponse(error="Helm installation failed", reason=e.stderr or str(e))
        except Exception as e:
            cleanup_cmd = [
                "helm", "uninstall", f"supabase-{request.namespace}", "-n", request.namespace
            ]
            subprocess.run(cleanup_cmd, capture_output=True, text=True)
            return ErrorResponse(error="Failed to create Vela configuration", reason=str(e))
        finally:
            os.unlink(temp_values_path)
    except Exception as e:
        return ErrorResponse(error="Failed to create Vela configuration", reason=str(e))

async def get_deployment_status(deployment_id: str, session: Session):
    deployment = session.get(Deployment, deployment_id)
    if not deployment:
        return ErrorResponse(error="Deployment not found", reason=f"Deployment ID {deployment_id} does not exist")
    ns = deployment.namespace
    k8s_status = kube_service.check_namespace_status(ns)
    deployment.status = k8s_status['status']
    session.commit()
    return DeploymentStatusResponse(
        deployment_id=deployment_id,
        namespace=ns,
        status=k8s_status['status'],
        pods=k8s_status['pods'],
        message=k8s_status['message']
    )

async def list_deployments(session: Session):
    try:
        deployments = session.exec(select(Deployment)).all()
        return [DeploymentItem(**d.to_dict()) for d in deployments]
    except Exception as e:
        return ErrorResponse(error="Failed to list deployments", reason=str(e))

async def delete_deployment(deployment_id: str, session: Session):
    deployment = session.get(Deployment, deployment_id)
    if not deployment:
        return ErrorResponse(error="Deployment not found", reason=f"Deployment ID {deployment_id} does not exist")
    release_name = deployment.release_name
    namespace = deployment.namespace
    import subprocess
    try:
        cmd = [
            "helm", "uninstall", release_name, "-n", namespace
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        session.delete(deployment)
        session.commit()
        return DeleteDeploymentResponse(status="deleted", deployment_id=deployment_id, helm_output=result.stdout)
    except subprocess.CalledProcessError as e:
        return ErrorResponse(error="Helm uninstall failed", reason=e.stderr or str(e))
    except Exception as e:
        return ErrorResponse(error="Failed to delete deployment", reason=str(e))

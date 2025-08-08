import os
import subprocess
import tempfile
import yaml
from pathlib import Path
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import Base, engine, get_db
from models import Deployment
from kubernetes_service import KubernetesService

app = FastAPI(title="Vela API", description="API for managing database configurations")

# Ensure DB tables exist
Base.metadata.create_all(bind=engine)

kube_service = KubernetesService()

class VelaCreateRequest(BaseModel):
    namespace: str
    dbuser: str
    dbname: str
    dbpassword: str
    vcpu: int
    ram: int  # MB
    db_storage: int  # GB
    iops: int

class ErrorResponse(BaseModel):
    error: str
    reason: str | None = None

class DeploymentCreateResponse(BaseModel):
    deployment_id: str

class DeploymentItem(BaseModel):
    id: str
    namespace: str
    release_name: str
    db_user: str
    db_name: str
    status: str
    created_at: str
    updated_at: str | None = None

class DeploymentStatusResponse(BaseModel):
    deployment_id: str
    namespace: str
    status: str
    pods: list
    message: str

@app.post("/api/vela/create", response_model=DeploymentCreateResponse, responses={400: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
async def create_vela_config(request: VelaCreateRequest, db: Session = Depends(get_db)):
    """
    Create Vela configuration with database credentials and deploy using Helm.
    Sets environment variables for database connection and installs Supabase chart.
    """
    try:
        # Set environment variables for database credentials
        os.environ["SECRET_DB_USERNAME"] = request.dbuser
        os.environ["SECRET_DB_DATABASE"] = request.dbname
        os.environ["SECRET_DB_PASSWORD"] = request.dbpassword
        
        # Log the configuration (without sensitive data)
        print(f"Creating Vela configuration for namespace: {request.namespace}")
        print(f"Database user: {request.dbuser}")
        print(f"Database name: {request.dbname}")
        print("Database password: [REDACTED]")
        
        # Get the project root directory (assuming code folder is in project root)
        project_root = Path(__file__).parent.parent
        chart_path = project_root / "charts" / "supabase"
        values_file = project_root / "charts" / "supabase" / "values.example.yaml"
        
        # Verify chart and values file exist
        if not chart_path.exists():
            raise HTTPException(status_code=404, detail=f"Supabase chart not found at {chart_path}")
        if not values_file.exists():
            raise HTTPException(status_code=404, detail=f"Values file not found at {values_file}")
        
        # Create a temporary values file with updated database credentials
        with open(values_file, 'r') as f:
            values_content = yaml.safe_load(f)
        
        # Update database credentials in values
        if 'db' not in values_content:
            values_content['db'] = {}
        values_content['db']['username'] = request.dbuser
        values_content['db']['database'] = request.dbname
        values_content['db']['password'] = request.dbpassword
        # Add provisioning limits to values (customize as needed for your chart)
        values_content['provisioning'] = {
            'vcpu': request.vcpu,
            'ram': request.ram,
            'db_storage': request.db_storage,
            'iops': request.iops
        }
        # Create temporary values file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_values:
            yaml.dump(values_content, temp_values, default_flow_style=False)
            temp_values_path = temp_values.name
        
        try:
            # Install Helm chart
            release_name = f"supabase-{request.namespace}"
            helm_cmd = [
                "helm", "install", release_name, str(chart_path),
                "--namespace", request.namespace,
                "--create-namespace",
                "-f", temp_values_path
            ]
            
            print(f"Running Helm command: {' '.join(helm_cmd)}")
            result = subprocess.run(helm_cmd, check=True, capture_output=True, text=True)
            print("Helm install output:")
            print(result.stdout)
            # Store deployment in DB
            deployment = Deployment(
                namespace=request.namespace,
                release_name=release_name,
                db_user=request.dbuser,
                db_name=request.dbname,
                status="deploying"
            )
            db.add(deployment)
            db.commit()
            db.refresh(deployment)
            return DeploymentCreateResponse(deployment_id=str(deployment.id))
        except subprocess.CalledProcessError as e:
            # Cleanup: helm uninstall if install failed
            cleanup_cmd = [
                "helm", "uninstall", f"supabase-{request.namespace}", "-n", request.namespace
            ]
            subprocess.run(cleanup_cmd, capture_output=True, text=True)
            return ErrorResponse(error="Helm installation failed", reason=e.stderr or str(e))
        except Exception as e:
            # Cleanup: helm uninstall if install failed
            cleanup_cmd = [
                "helm", "uninstall", f"supabase-{request.namespace}", "-n", request.namespace
            ]
            subprocess.run(cleanup_cmd, capture_output=True, text=True)
            return ErrorResponse(error="Failed to create Vela configuration", reason=str(e))
        finally:
            os.unlink(temp_values_path)
    except Exception as e:
        return ErrorResponse(error="Failed to create Vela configuration", reason=str(e))

@app.get("/api/vela/status/{deployment_id}", response_model=DeploymentStatusResponse, responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
async def get_deployment_status(deployment_id: str, db: Session = Depends(get_db)):
    deployment = db.query(Deployment).filter(Deployment.id == deployment_id).first()
    if not deployment:
        return ErrorResponse(error="Deployment not found", reason=f"Deployment ID {deployment_id} does not exist")
    ns = deployment.namespace
    k8s_status = kube_service.check_namespace_status(ns)
    # Optionally update status in DB
    deployment.status = k8s_status['status']
    db.commit()
    return DeploymentStatusResponse(
        deployment_id=deployment_id,
        namespace=ns,
        status=k8s_status['status'],
        pods=k8s_status['pods'],
        message=k8s_status['message']
    )

@app.get("/api/vela/deployments", response_model=list[DeploymentItem], responses={500: {"model": ErrorResponse}})
async def list_deployments(db: Session = Depends(get_db)):
    try:
        deployments = db.query(Deployment).all()
        return [DeploymentItem(**d.to_dict()) for d in deployments]
    except Exception as e:
        return ErrorResponse(error="Failed to list deployments", reason=str(e))

class DeleteDeploymentResponse(BaseModel):
    status: str
    deployment_id: str
    helm_output: str

@app.delete("/api/vela/deployments/{deployment_id}", response_model=DeleteDeploymentResponse, responses={404: {"model": ErrorResponse}, 500: {"model": ErrorResponse}})
async def delete_deployment(deployment_id: str, db: Session = Depends(get_db)):
    deployment = db.query(Deployment).filter(Deployment.id == deployment_id).first()
    if not deployment:
        return ErrorResponse(error="Deployment not found", reason=f"Deployment ID {deployment_id} does not exist")
    # Run helm uninstall
    release_name = deployment.release_name
    namespace = deployment.namespace
    try:
        cmd = [
            "helm", "uninstall", release_name, "-n", namespace
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        db.delete(deployment)
        db.commit()
        return DeleteDeploymentResponse(status="deleted", deployment_id=deployment_id, helm_output=result.stdout)
    except subprocess.CalledProcessError as e:
        return ErrorResponse(error="Helm uninstall failed", reason=e.stderr or str(e))
    except Exception as e:
        return ErrorResponse(error="Failed to delete deployment", reason=str(e))


@app.get("/")
async def root():
    return {"message": "Vela API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

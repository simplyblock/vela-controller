# GCP Terraform Deployment

This module creates a VPC with a public subnetwork and a VPC‑native GKE cluster with a primary node pool.

## Prerequisites
- Terraform >= 1.6
- `gcloud` CLI authenticated (`gcloud auth login` or service account with application default credentials)
- AWS/S3-compatible credentials for the remote backend

## Usage
```bash
cd deployment/gcp
terraform init -reconfigure
terraform apply
```

## Retrieve kubeconfig
After the cluster finishes provisioning, fetch kubeconfig:
```bash
gcloud container clusters get-credentials ${cluster_name} --region ${region} --project ${project_id}
```

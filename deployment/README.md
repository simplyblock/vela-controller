Establish variables for deployment in `./cluster/terraform.tfvars`.
For an example see `./cluster/terraform.tfvars.example`.

Basic usage:
```
export PROXMOX_VE_USERNAME=<user>
export PROXMOX_VE_PASSWORD=<password>
cd cluster
terraform init
terraform apply
terraform output -raw kubeconfig > kubeconfig.yaml

cd ../addons
terraform init
terraform apply
```

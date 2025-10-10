Establish variables for deployment in `./cluster/terraform.tfvars`.
For an example see `./cluster/terraform.tfvars.example`.

Please make sure to update `proxmox-host` with the IP of the proxmox host. And `proxmox_node` with the name of the proxmox node.
You might also have to change the `ip_range_start` to avoid conflicts with existing IP addresses within the Simplyblock VPN.

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

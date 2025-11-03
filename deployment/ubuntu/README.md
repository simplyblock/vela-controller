# Ubuntu VM Terraform Module

This Terraform configuration provisions a configurable number of Ubuntu virtual machines on Proxmox. It is intended for lightweight lab or staging environments where you only need basic VMs without Talos.

## Prerequisites

- Proxmox VE instance with API access enabled.
- Terraform `>= 1.5`.
- The Proxmox Terraform provider credentials exported in your shell:
  ```bash
  export PROXMOX_VE_USERNAME=<user@pam>
  export PROXMOX_VE_PASSWORD=<password>
  ```

## Usage

1. Copy the example variables file and adjust the values to match your environment:
   ```bash
   cd deployment/ubuntu
   cp terraform.tfvars.example terraform.tfvars
   ```
2. Review `terraform.tfvars`, especially the network, datastore, and SSH settings.
3. Initialize and apply:
   ```bash
   terraform init
   terraform apply
   ```
4. Outputs include the VM names and the IPv4 addresses that were assigned.

## Customization

- `vm_count` controls how many VMs are created (defaults to three).
- Provide SSH keys via `ssh_public_keys` to enable passwordless login for the cloud-init user.
- Override the Ubuntu cloud image if you prefer another release, or supply your own image mirror.

To destroy the environment:

```bash
terraform destroy
```

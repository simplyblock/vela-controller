provider "proxmox" {
  endpoint = var.proxmox_endpoint
  insecure = true

  ssh {
    agent    = true
    username = "root"
  }
}

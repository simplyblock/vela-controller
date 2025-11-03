resource "proxmox_virtual_environment_vm" "ubuntu" {
  count       = var.vm_count
  name        = local.vm_names[count.index]
  description = "Ubuntu VM ${count.index + 1} - Managed by Terraform"
  tags        = local.computed_vm_tags
  node_name   = var.proxmox_node
  on_boot     = true

  cpu {
    cores = var.vm_cpu_cores
    type  = "host"
  }

  memory {
    dedicated = var.vm_memory_mb
  }

  agent {
    enabled = true
  }

  network_device {
    bridge = var.network_bridge
  }

  disk {
    datastore_id = var.datastore_id
    file_id      = proxmox_virtual_environment_download_file.ubuntu_cloud_image.id
    file_format  = var.ubuntu_cloud_image_file_format
    interface    = "scsi0"
    size         = var.vm_disk_size_gb
  }

  operating_system {
    type = "l26"
  }

  initialization {
    datastore_id = var.datastore_id

    ip_config {
      ipv4 {
        address = "${local.vm_ips[count.index]}/${var.network_cidr}"
        gateway = var.default_gateway
      }
    }

    dynamic "dns" {
      for_each = length(var.dns_servers) > 0 ? [1] : []
      content {
        servers = var.dns_servers
      }
    }

    dynamic "user_account" {
      for_each = (length(var.ssh_public_keys) > 0 || var.vm_password != null) ? [1] : []
      content {
        username = var.vm_username
        keys     = var.ssh_public_keys
        password = var.vm_password
      }
    }
  }

}

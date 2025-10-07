resource "proxmox_virtual_environment_vm" "talos_control_plane" {
  count       = var.control_plane_count
  name        = "talos-cp-${format("%02d", count.index + 1)}"
  description = "Talos Control Plane Node ${count.index + 1} - Managed by Terraform"
  tags        = ["${var.cluster_name}"]
  node_name   = var.proxmox_node
  on_boot     = true

  cpu {
    cores = var.cp_cpu_cores
    type  = "host"
  }

  memory {
    dedicated = var.cp_memory_mb
  }

  agent {
    enabled = true
  }

  network_device {
    bridge = var.network_bridge
  }

  disk {
    datastore_id = var.datastore_id
    file_id      = proxmox_virtual_environment_download_file.talos_nocloud_image.id
    file_format  = "raw"
    interface    = "virtio0"
    size         = var.cp_disk_size_gb
  }

  operating_system {
    type = "l26" # Linux Kernel 2.6 - 5.X.
  }

  initialization {
    datastore_id = var.datastore_id
    ip_config {
      ipv4 {
        address = "${local.cp_ips[count.index]}/${var.network_cidr}"
        gateway = var.default_gateway
      }
      ipv6 {
        address = "auto" # Use SLAAC instead of DHCP
      }
    }
  }

  lifecycle {
    ignore_changes = [
      initialization[0].ip_config[0].ipv6[0].address
    ]
  }
}

resource "proxmox_virtual_environment_vm" "talos_worker" {
  count       = var.worker_count
  depends_on  = [proxmox_virtual_environment_vm.talos_control_plane]
  name        = "talos-worker-${format("%02d", count.index + 1)}"
  description = "Talos Worker Node ${count.index + 1} - Managed by Terraform"
  tags        = ["${var.cluster_name}"]
  node_name   = var.proxmox_node
  on_boot     = true

  cpu {
    cores = var.worker_cpu_cores
    type  = "host"
  }

  memory {
    dedicated = var.worker_memory_mb
  }

  agent {
    enabled = true
  }

  network_device {
    bridge = var.network_bridge
  }

  disk {
    datastore_id = var.datastore_id
    file_id      = proxmox_virtual_environment_download_file.talos_nocloud_image.id
    file_format  = "raw"
    interface    = "virtio0"
    size         = var.worker_disk_size_gb
  }

  operating_system {
    type = "l26" # Linux Kernel 2.6 - 5.X.
  }

  initialization {
    datastore_id = var.datastore_id
    ip_config {
      ipv4 {
        address = "${local.worker_ips[count.index]}/${var.network_cidr}"
        gateway = var.default_gateway
      }
      ipv6 {
        address = "auto" # Use SLAAC instead of DHCP
      }
    }
  }

  lifecycle {
    ignore_changes = [
      initialization[0].ip_config[0].ipv6[0].address
    ]
  }
}

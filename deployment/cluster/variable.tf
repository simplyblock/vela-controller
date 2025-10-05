variable "proxmox_endpoint" {
  type        = string
  description = "Proxmox API endpoint"
}

variable "proxmox_node" {
  type        = string
  description = "Proxmox node name"
}

variable "cluster_name" {
  type        = string
  default     = "vela-deployment"
  description = "Name of the Talos cluster"
}

variable "default_gateway" {
  type        = string
  description = "Default gateway for the network"
}

variable "control_plane_count" {
  type        = number
  default     = 1
  description = "Number of control plane nodes"
  validation {
    condition     = var.control_plane_count > 0
    error_message = "Control plane count must be greater than 0."
  }
}

variable "worker_count" {
  type        = number
  default     = 1
  description = "Number of worker nodes"
  validation {
    condition     = var.worker_count >= 0
    error_message = "Worker count must be greater than or equal to 0."
  }
}

variable "ip_range_start" {
  type        = string
  description = "Starting IP address for the cluster nodes"
}

output "ip_range_start" {
  value = var.ip_range_start
}

variable "ip_range_end" {
  type        = string
  description = "Ending IP address for the cluster nodes (for validation)"
}

variable "network_cidr" {
  type        = string
  default     = "24"
  description = "Network CIDR (subnet mask)"
}

variable "hypervisor_ipv6_subnet" {
  type        = string
  description = "IPv6 subnet of the hypervisor"
}

# Control plane node specifications
variable "cp_cpu_cores" {
  type        = number
  default     = 2
  description = "CPU cores for control plane nodes"
}

variable "cp_memory_mb" {
  type        = number
  default     = 4096
  description = "Memory in MB for control plane nodes"
}

variable "cp_disk_size_gb" {
  type        = number
  default     = 20
  description = "Disk size in GB for control plane nodes"
}

# Worker node specifications
variable "worker_cpu_cores" {
  type        = number
  default     = 4
  description = "CPU cores for worker nodes"
}

variable "worker_memory_mb" {
  type        = number
  default     = 8192
  description = "Memory in MB for worker nodes"
}

variable "worker_disk_size_gb" {
  type        = number
  default     = 20
  description = "Disk size in GB for worker nodes"
}

variable "datastore_id" {
  type        = string
  default     = "local-lvm-thin"
  description = "Proxmox datastore ID"
}

variable "network_bridge" {
  type        = string
  default     = "vmbr0"
  description = "Network bridge name"
}

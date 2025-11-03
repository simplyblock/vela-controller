variable "proxmox_endpoint" {
  type        = string
  description = "Proxmox API endpoint (e.g. https://proxmox-host:8006/api2/json)."
}

variable "proxmox_node" {
  type        = string
  description = "Proxmox node name where the VMs will be created."
}

variable "vm_count" {
  type        = number
  default     = 3
  description = "Number of Ubuntu VMs to provision."

  validation {
    condition     = var.vm_count > 0
    error_message = "vm_count must be greater than 0."
  }
}

variable "vm_base_name" {
  type        = string
  default     = "ubuntu-vm"
  description = "Base name for the Ubuntu VMs (a numeric suffix will be appended)."
}

variable "vm_tags" {
  type        = list(string)
  default     = []
  description = "Optional list of tags to add to the VMs."
}

variable "vm_cpu_cores" {
  type        = number
  default     = 2
  description = "Number of CPU cores for each VM."
}

variable "vm_memory_mb" {
  type        = number
  default     = 4096
  description = "Memory allocation (in MB) for each VM."
}

variable "vm_disk_size_gb" {
  type        = number
  default     = 40
  description = "Disk size (in GB) for each VM."
}

variable "datastore_id" {
  type        = string
  default     = "local-lvm-thin"
  description = "Proxmox datastore ID used for VM disks."
}

variable "image_datastore_id" {
  type        = string
  default     = "local"
  description = "Proxmox datastore ID used for storing the downloaded cloud image."
}

variable "network_bridge" {
  type        = string
  default     = "vmbr0"
  description = "Network bridge to attach the VMs to."
}

variable "default_gateway" {
  type        = string
  description = "Default gateway for the VM network."
}

variable "ip_range_start" {
  type        = string
  description = "Starting IPv4 address that will be assigned to the first VM."
}

variable "network_cidr" {
  type        = string
  default     = "24"
  description = "Subnet mask (CIDR) for the VM network."
}

variable "dns_servers" {
  type        = list(string)
  default     = []
  description = "Optional list of DNS servers to configure via cloud-init."
}

variable "vm_username" {
  type        = string
  default     = "ubuntu"
  description = "Username that will be created in the VM via cloud-init."
}

variable "vm_password" {
  type        = string
  default     = null
  sensitive   = true
  description = "Optional password for the VM user. Leave null when using SSH keys."
}

variable "ssh_public_keys" {
  type        = list(string)
  default     = []
  description = "List of SSH public keys to inject via cloud-init for the VM user."
}

variable "ubuntu_cloud_image_url" {
  type        = string
  default     = "https://cloud-images.ubuntu.com/jammy/current/jammy-server-cloudimg-amd64.img"
  description = "URL of the Ubuntu cloud image to download."
}

variable "ubuntu_cloud_image_file_name" {
  type        = string
  default     = "jammy-server-cloudimg-amd64.img"
  description = "File name to use when storing the Ubuntu cloud image in Proxmox."
}

variable "ubuntu_cloud_image_file_format" {
  type        = string
  default     = "qcow2"
  description = "Format of the downloaded Ubuntu cloud image."
}

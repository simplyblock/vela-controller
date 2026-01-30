variable "project_id" {
  type        = string
  description = "GCP project ID to deploy resources into."
  default = "sadegh-sb"
}

variable "region" {
  type        = string
  description = "Default region for resources."
  default     = "us-central1"
}

variable "zone" {
  type        = string
  description = "Default zone within the region."
  default     = "us-central1-a"
}

variable "credentials_file" {
  type        = string
  description = "Path to the service account JSON key (leave blank to use ADC)."
  default     = ""
}

variable "network_name" {
  type        = string
  description = "Name of the VPC to create."
  default     = "vela-network"
}

variable "subnetwork_name" {
  type        = string
  description = "Name of the public subnetwork."
  default     = "vela-public-subnet"
}

variable "subnetwork_cidr" {
  type        = string
  description = "CIDR block for the public subnetwork."
  default     = "10.10.0.0/16"
}

variable "pods_secondary_cidr" {
  type        = string
  description = "Secondary CIDR for GKE pods (VPC-native)."
  default     = "10.20.0.0/16"
}

variable "services_secondary_cidr" {
  type        = string
  description = "Secondary CIDR for GKE services (VPC-native)."
  default     = "10.30.0.0/20"
}

variable "cluster_name" {
  type        = string
  description = "Name of the GKE cluster."
  default     = "vela-gke"
}

variable "node_count" {
  type        = number
  description = "Number of nodes in the primary node pool."
  default     = 3
}

variable "node_machine_type" {
  type        = string
  description = "Machine type for GKE nodes."
  default     = "e2-standard-4"
}

variable "node_disk_size_gb" {
  type        = number
  description = "Boot disk size for each node (GB)."
  default     = 100
}

variable "node_service_account" {
  type        = string
  description = "Service account email for nodes. Leave blank to use the default compute service account."
  default     = ""
}

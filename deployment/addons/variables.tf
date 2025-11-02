variable "simplyblock_cluster_id" {
  description = "Simplyblock cluster id"
  type        = string
  sensitive   = true
}

variable "simplyblock_cluster_secret" {
  description = "Simplyblock cluster secret"
  type        = string
  sensitive   = true
}

variable "simplyblock_pool_name" {
  description = "Simplyblock storage pool name"
  type        = string
  sensitive   = true
}

variable "simplyblock_endpoint" {
  description = "SimplyBlock endpoint"
  type        = string
}


variable "kubevirt_version" {
  description = "KubeVirt version to deploy"
  type        = string
  default     = "v1.7.0-beta.0"
}

variable "external_ip" {
  description = "External IP address to use"
  type        = string
}

variable "kube_ovn_master_node_name" {
  description = "Name of the node that should be labeled as the Kube-OVN master"
  type        = string
}

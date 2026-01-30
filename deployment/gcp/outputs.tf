output "network" {
  description = "VPC network name."
  value       = google_compute_network.vpc.name
}

output "subnetwork" {
  description = "Public subnetwork name."
  value       = google_compute_subnetwork.public.name
}

output "gke_cluster_name" {
  description = "GKE cluster name."
  value       = google_container_cluster.gke.name
}

output "gke_endpoint" {
  description = "GKE control plane endpoint."
  value       = google_container_cluster.gke.endpoint
}

output "gke_ca_certificate" {
  description = "Base64-encoded cluster CA certificate."
  value       = google_container_cluster.gke.master_auth[0].cluster_ca_certificate
  sensitive   = true
}

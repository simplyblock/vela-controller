terraform {
  required_version = ">= 1.6.0"

  backend "gcs" {
    bucket  = "vela-terraform-state"
    prefix  = "gcp/terraform.tfstate"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.33"
    }
  }
}


# VPC with a single public subnetwork and secondary ranges for VPC-native GKE.
resource "google_compute_network" "vpc" {
  name                    = var.network_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "public" {
  name          = var.subnetwork_name
  ip_cidr_range = var.subnetwork_cidr
  region        = var.region
  network       = google_compute_network.vpc.id

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_secondary_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_secondary_cidr
  }
}

# Regional GKE cluster using the public subnetwork.
resource "google_container_cluster" "gke" {
  name     = var.cluster_name
  location = var.region

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.public.name

  remove_default_node_pool = true
  initial_node_count       = 1

  networking_mode = "VPC_NATIVE"

  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = "REGULAR"
  }

  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }
}

resource "google_container_node_pool" "primary_nodes" {
  name       = "${var.cluster_name}-pool"
  cluster    = google_container_cluster.gke.name
  location   = google_container_cluster.gke.location
  node_count = var.node_count

  node_config {
    machine_type = var.node_machine_type
    image_type   = "UBUNTU_CONTAINERD"
    disk_size_gb = var.node_disk_size_gb

    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]

    metadata = {
      disable-legacy-endpoints = "true"
    }

    service_account = var.node_service_account != "" ? var.node_service_account : null
    labels = {
      env = "vela"
    }
    tags = ["vela-gke-node"]
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

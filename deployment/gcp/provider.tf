provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone
  credentials = var.credentials_file != "" ? file(var.credentials_file) : null
}

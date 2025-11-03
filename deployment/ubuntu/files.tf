resource "proxmox_virtual_environment_download_file" "ubuntu_cloud_image" {
  content_type = "iso"
  datastore_id = var.image_datastore_id
  node_name    = var.proxmox_node
  file_name    = var.ubuntu_cloud_image_file_name
  url          = var.ubuntu_cloud_image_url
  overwrite    = true
}

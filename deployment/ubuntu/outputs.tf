output "vm_names" {
  value       = local.vm_names
  description = "Provisioned Ubuntu VM names."
}

output "vm_ips" {
  value       = local.vm_ips
  description = "Assigned IPv4 addresses for the Ubuntu VMs."
}

locals {
  ip_octets           = split(".", var.ip_range_start)
  base_ip             = format("%s.%s.%s", local.ip_octets[0], local.ip_octets[1], local.ip_octets[2])
  start_ip_last_octet = tonumber(local.ip_octets[3])
  vm_ips              = [for i in range(var.vm_count) : format("%s.%d", local.base_ip, local.start_ip_last_octet + i)]
  computed_vm_tags    = length(var.vm_tags) > 0 ? var.vm_tags : ["ubuntu"]
  vm_name_suffix      = [for i in range(var.vm_count) : format("%02d", i + 1)]
  vm_names            = [for suffix in local.vm_name_suffix : "${var.vm_base_name}-${suffix}"]
}

locals {
  talos = {
    version = "v1.7.4"
  }

  # Calculate IP addresses based on start IP and node counts
  ip_octets = split(".", var.ip_range_start)
  base_ip   = format("%s.%s.%s", local.ip_octets[0], local.ip_octets[1], local.ip_octets[2])
  start_ip_last_octet = tonumber(local.ip_octets[3])

  # Generate IP addresses for control plane nodes
  cp_ips = [
    for i in range(var.control_plane_count) :
    format("%s.%d", local.base_ip, local.start_ip_last_octet + i)
  ]

  # Generate IP addresses for worker nodes
  worker_ips = [
    for i in range(var.worker_count) :
    format("%s.%d", local.base_ip, local.start_ip_last_octet + var.control_plane_count + i)
  ]

  # All node IPs combined
  all_node_ips = concat(local.cp_ips, local.worker_ips)

  # Primary control plane IP (first CP node)
  primary_cp_ip = length(local.cp_ips) > 0 ? local.cp_ips[0] : ""
}

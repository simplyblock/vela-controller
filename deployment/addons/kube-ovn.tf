resource "kubectl_manifest" "kube_ovn_master_label" {
  yaml_body = yamlencode({
    apiVersion = "v1"
    kind       = "Node"
    metadata = {
      name = var.kube_ovn_master_node_name
      labels = {
        "kube-ovn/role" = "master"
      }
    }
  })
}

resource "helm_release" "kube_ovn" {
  name       = "kube-ovn"
  repository = "https://kubeovn.github.io/kube-ovn"
  chart      = "kube-ovn"
  namespace  = "kube-system"
  version    = "v1.14.10"
  wait       = true

  values = [
    yamlencode({
      OVN_DIR                    = "/var/lib/ovn"
      OPENVSWITCH_DIR            = "/var/lib/openvswitch"
      DISABLE_MODULES_MANAGEMENT = true
      cni_conf = {
        MOUNT_LOCAL_BIN_DIR = false
      }
    })
  ]

  depends_on = [kubectl_manifest.kube_ovn_master_label]
}

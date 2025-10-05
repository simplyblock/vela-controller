data "http" "metallb_manifest" {
  url = "https://raw.githubusercontent.com/metallb/metallb/v0.15.2/config/manifests/metallb-native.yaml"
}

data "kubectl_file_documents" "metallb_docs" {
  content = data.http.metallb_manifest.response_body
}

resource "kubernetes_namespace" "metallb_namespace" {
  metadata {
    name = "metallb-system"
  }
}


resource "kubectl_manifest" "metallb_install" {
  for_each   = data.kubectl_file_documents.metallb_docs.manifests
  yaml_body  = each.value
  wait       = true

  depends_on = [kubernetes_namespace.metallb_namespace]
}

resource "kubectl_manifest" "metallb_ipaddresspool" {
  yaml_body = yamlencode({
    apiVersion = "metallb.io/v1beta1"
    kind       = "IPAddressPool"
    metadata = {
      name      = "default-address-pool"
      namespace = "metallb-system"
    }
    spec = {
      addresses = [
        format("%s-%s", var.external_ip, var.external_ip)
      ]
    }
  })

  depends_on = [kubectl_manifest.metallb_install]
}

resource "kubectl_manifest" "metallb_l2advertisement" {
  yaml_body = yamlencode({
    apiVersion = "metallb.io/v1beta1"
    kind       = "L2Advertisement"
    metadata = {
      name      = "advert"
      namespace = "metallb-system"
    }
    spec = {}
  })

  depends_on = [kubectl_manifest.metallb_ipaddresspool]
}

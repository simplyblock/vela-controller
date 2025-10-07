data "http" "kubevirt_operator" {
  url = "https://github.com/kubevirt/kubevirt/releases/download/${var.kubevirt_version}/kubevirt-operator.yaml"
}

data "kubectl_file_documents" "kubevirt_operator" {
  content = data.http.kubevirt_operator.response_body
}

resource "kubectl_manifest" "kubevirt_operator" {
  for_each   = data.kubectl_file_documents.kubevirt_operator.manifests
  yaml_body  = each.value
  depends_on = []
}

resource "time_sleep" "wait_for_operator" {
  depends_on      = [kubectl_manifest.kubevirt_operator]
  create_duration = "60s"
}

resource "kubectl_manifest" "kubevirt_cr" {
  yaml_body = yamlencode({
    apiVersion = "kubevirt.io/v1"
    kind       = "KubeVirt"
    metadata = {
      name      = "kubevirt"
      namespace = "kubevirt"
    }
    spec = {
      certificateRotateStrategy = {}
      configuration = {
        developerConfiguration = {
          useEmulation = true
        }
      }
      workloadUpdateStrategy = {
        workloadUpdateMethods = ["LiveMigrate"]
        batchEvictionSize     = 10
        batchEvictionInterval = "1m0s"
      }
    }
  })

  depends_on = [time_sleep.wait_for_operator]
}

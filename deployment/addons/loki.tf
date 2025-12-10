resource "kubernetes_namespace" "loki" {
  metadata {
    name = "loki"
    labels = {
      "pod-security.kubernetes.io/enforce" = "privileged"
      "pod-security.kubernetes.io/audit"   = "privileged"
      "pod-security.kubernetes.io/warn"    = "privileged"
    }
  }
}

resource "helm_release" "loki" {
  name       = "loki"
  repository = "https://grafana.github.io/helm-charts"
  chart      = "grafana/loki"
  namespace  = "loki"
  version    = "v3.6.2"
  wait       = true

  create_namespace = true

  values = [templatefile("loki.yaml", {})]

  depends_on = [kubernetes_namespace.loki]
}

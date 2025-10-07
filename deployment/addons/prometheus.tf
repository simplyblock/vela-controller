resource "helm_release" "prometheus" {
  name             = "prometheus"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "prometheus"
  namespace        = "monitoring"
  create_namespace = true
  version          = "25.18.0"

  set {
    name  = "prometheus.server.fullnameOverride"
    value = "vela-prometheus"
  }

  set {
    name  = "prometheus.server.replicaCount"
    value = 1
  }

  set {
    name  = "prometheus.server.statefulSet.enabled"
    value = true
  }

  set {
    name  = "prometheus.server.persistentVolume.enabled"
    value = true
  }

  set {
    name  = "prometheus.server.persistentVolume.size"
    value = "5Gi"
  }
}

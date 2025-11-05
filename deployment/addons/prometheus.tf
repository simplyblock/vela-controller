resource "helm_release" "prometheus" {
  name             = "prometheus"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "prometheus"
  namespace        = "monitoring"
  create_namespace = true
  version          = "25.18.0"

  set {
    name  = "server.fullnameOverride"
    value = "vela-prometheus"
  }

  set {
    name  = "server.replicaCount"
    value = 1
  }

  set {
    name  = "server.statefulSet.enabled"
    value = true
  }

  set {
    name  = "server.persistentVolume.enabled"
    value = true
  }

  set {
    name  = "server.persistentVolume.size"
    value = "5Gi"
  }
  set {
    name  = "server.service.servicePort"
    value = 9090
  }
  
  set {
    name  = "server.configMapOverrideName"
    value = "vela-prometheus-config"
  }
}

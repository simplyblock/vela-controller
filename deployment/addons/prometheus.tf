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
  
  set {
    name  = "alertmanager.enabled"
    value = false
  }

  set {
    name  = "prometheus-pushgateway.enabled"
    value = false
  }

  set {
    name  = "prometheus-node-exporter.enabled"
    value = false
  }

  set {
    name  = "kube-state-metrics.enabled"
    value = false
  }
}

resource "kubernetes_config_map" "vela_prometheus_config" {
  metadata {
    name      = "prometheus-vela-prometheus-config"
    namespace = "monitoring"
    labels = {
      app = "vela-prometheus"
    }
  }

  data = {
    "prometheus.yml" = <<-YAML
      global:
        scrape_interval: 15s
        external_labels:
          monitor: 'codelab-monitor'

      scrape_configs:
        - job_name: "postgres"
          kubernetes_sd_configs:
            - role: service
          relabel_configs:
            - source_labels: [__meta_kubernetes_service_label_app]
              regex: pgexporter
              action: keep

            - source_labels: [__meta_kubernetes_namespace]
              target_label: namespace
    YAML
  }
}

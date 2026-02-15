resource "helm_release" "prometheus" {
  name             = "prometheus"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "prometheus"
  namespace        = var.namespace
  create_namespace = true
  version          = "25.18.0"

  set {
    name  = "server.fullnameOverride"
    value = "vela-prometheus"
  }

  set {
    name  = "server.replicaCount"
    value = 3
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
    name  = "server.extraVolumes"
    value = <<EOF
  - name: objstore-config
    configMap:
      name: vela-objstore-config
  EOF
  }

  set {
    name  = "server.affinity"
    value = <<EOF
  podAntiAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      - labelSelector:
          matchExpressions:
            - key: app.kubernetes.io/component
              operator: In
              values:
                - vela-prometheus
        topologyKey: "kubernetes.io/hostname"
  EOF
  }

  set {
    name  = "server.sidecarContainers"
    value = <<EOF
  thanos-sidecar:
    image: "thanosio/thanos:v0.31.0"
    args:
      - sidecar
      - --tsdb.path=/prometheus
      - --prometheus.url=http://localhost:9090
      - --objstore.config-file=/etc/thanos/objstore.yml
    ports:
      - name: grpc
        containerPort: 10901
      - name: http
        containerPort: 10902
    volumeMounts:
      - name: storage-volume
        mountPath: /prometheus
      - name: objstore-config
        mountPath: /etc/thanos
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
      limits:
        cpu: "250m"
        memory: "1Gi"
  EOF
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
    namespace = var.namespace
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

resource "kubernetes_config_map" "vela_objstore_config" {
  metadata {
    name      = "vela-objstore-config"
    namespace = var.namespace
    labels = {
      app = "vela-thanos"
    }
  }

  data = {
    "objstore.yml" = <<EOF
type: FILESYSTEM
config:
  directory: /mnt/thanos
EOF
  }
}

resource "kubernetes_service" "vela_thanos_store" {
  metadata {
    name      = "vela-thanos-store"
    namespace = var.namespace
    labels = {
      app = "vela-thanos-store"
    }
  }

  spec {
    selector = {
      app = "vela-thanos-store"
    }

    port {
      name       = "thanos-store"
      port       = 10901
      target_port = 10901
      protocol   = "TCP"
    }

    type = "ClusterIP"
  }
}

resource "kubernetes_deployment" "vela_thanos_store" {
  metadata {
    name      = "vela-thanos-store"
    namespace = var.namespace
    labels = {
      app = "vela-thanos-store"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "vela-thanos-store"
      }
    }

    template {
      metadata {
        labels = {
          app = "vela-thanos-store"
        }
      }

      spec {
        container {
          name  = "thanos-store"
          image = "thanosio/thanos:v0.31.0"

          args = [
            "store",
            "--objstore.config-file=/etc/thanos/objstore.yml",
            "--index-cache-size=500MB",
            "--chunk-pool-size=500MB",
          ]

          resources {
            limits = {
              cpu    = "250m"
              memory = "1Gi"
            }
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
          }

          volume_mount {
            name       = "objstore-config"
            mount_path = "/etc/thanos"
          }

          volume_mount {
            name       = "thanos-data"
            mount_path = "/data"
          }
        }

        volume {
          name = "objstore-config"
          config_map {
            name = "vela-objstore-config"
          }
        }

        volume {
          name = "thanos-data"
          empty_dir {}
        }
      }
    }
  }
}

resource "kubernetes_service" "vela_thanos_query" {
  metadata {
    name      = "vela-thanos-query"
    namespace = var.namespace
  }

  spec {
    selector = {
      app = "vela-thanos-query"
    }

    port {
      name       = "thanos-query"
      port       = 9091
      target_port = 9091
    }
  }
}

resource "kubernetes_deployment" "vela_thanos_query" {
  metadata {
    name      = "vela-thanos-query"
    namespace = var.namespace
    labels = {
      app = "vela-thanos-query"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "vela-thanos-query"
      }
    }

    template {
      metadata {
        labels = {
          app = "vela-thanos-query"
        }
      }

      spec {
        container {
          name  = "thanos-query"
          image = "thanosio/thanos:v0.31.0"

          args = [
            "query",
            "--http-address=0.0.0.0:9091",
            "--store=vela-thanos-store:10901",
            "--store=vela-prometheus:10901",
          ]

          resources {
            limits = {
              cpu    = "250m"
              memory = "1Gi"
            }
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_deployment" "vela_thanos_compactor" {
  metadata {
    name      = "vela-thanos-compactor"
    namespace = var.namespace
    labels = {
      app = "vela-thanos-compactor"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "vela-thanos-compactor"
      }
    }

    template {
      metadata {
        labels = {
          app = "vela-thanos-compactor"
        }
      }

      spec {
        container {
          name  = "thanos-compactor"
          image = "thanosio/thanos:v0.31.0"

          args = [
            "compact",
            "--data-dir=/data",
            "--objstore.config-file=/etc/thanos/objstore.yml",
            "--retention.resolution-raw=30d",
            "--retention.resolution-5m=60d",
            "--retention.resolution-1h=90d",
            "--compact.concurrency=1",
            "--wait",
          ]

          volume_mount {
            name       = "objstore-config"
            mount_path = "/etc/thanos"
          }

          volume_mount {
            name       = "compactor-data"
            mount_path = "/data"
          }

          resources {
            limits = {
              cpu    = "250m"
              memory = "1Gi"
            }
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
          }
        }

        volume {
          name = "objstore-config"
          config_map {
            name = "vela-objstore-config"
          }
        }

        volume {
          name = "compactor-data"
          empty_dir {}
        }
      }
    }
  }
}

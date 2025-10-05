resource "kubernetes_namespace" "simplyblock_csi" {
  metadata {
    name = "simplyblock-csi"
    labels = {
      "pod-security.kubernetes.io/enforce" = "privileged"
      "pod-security.kubernetes.io/audit"   = "privileged" 
      "pod-security.kubernetes.io/warn"    = "privileged"
    }
  }
}

resource "helm_release" "simplyblock_csi" {
  name       = "simplyblock-csi"
  repository = "https://install.simplyblock.io/helm"
  chart      = "spdk-csi"
  namespace  = "simplyblock-csi"

  create_namespace = true

  set {
    name  = "csiConfig.simplybk.ip"
    value = var.simplyblock_endpoint
  }

  set {
    name  = "csiConfig.simplybk.uuid"
    value = var.simplyblock_cluster_id
  }

  set_sensitive {
    name  = "csiSecret.simplybk.secret"
    value = var.simplyblock_cluster_secret
  }

  set {
    name  = "logicalVolume.pool_name"
    value = var.simplyblock_pool_name
  }

  depends_on = [kubernetes_namespace.simplyblock_csi]
}

resource "kubernetes_annotations" "simplyblock_default_sc" {
  api_version = "storage.k8s.io/v1"
  kind        = "StorageClass"

  metadata {
    name = "simplyblock-csi-sc"
  }

  annotations = {
    "storageclass.kubernetes.io/is-default-class" = "true"
  }

  depends_on = [helm_release.simplyblock_csi]
}

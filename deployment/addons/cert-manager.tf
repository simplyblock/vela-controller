resource "kubernetes_namespace" "cert_manager" {
  metadata {
    name = "cert-manager"
  }
}

resource "helm_release" "cert_manager" {
  name       = "cert-manager"
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  version    = "v1.13.0"
  namespace  = kubernetes_namespace.cert_manager.metadata[0].name

  set {
    name  = "installCRDs"
    value = "true"
  }
}

resource "kubectl_manifest" "selfsigned_issuer" {
  yaml_body = yamlencode({
    apiVersion = "cert-manager.io/v1"
    kind       = "ClusterIssuer"
    metadata = {
      name = "selfsigned-issuer"
    }
    spec = {
      selfSigned = {}
    }
  })

  depends_on = [helm_release.cert_manager]
}

resource "kubectl_manifest" "ca_certificate" {
  yaml_body = yamlencode({
    apiVersion = "cert-manager.io/v1"
    kind       = "Certificate"
    metadata = {
      name      = "selfsigned-ca"
      namespace = kubernetes_namespace.cert_manager.metadata[0].name
    }
    spec = {
      isCA       = true
      commonName = "selfsigned-ca"
      secretName = "root-secret"
      privateKey = {
        algorithm = "ECDSA"
        size      = 256
      }
      issuerRef = {
        name  = "selfsigned-issuer"
        kind  = "ClusterIssuer"
        group = "cert-manager.io"
      }
    }
  })

  depends_on = [kubectl_manifest.selfsigned_issuer]
}

resource "kubectl_manifest" "ca_issuer" {
  yaml_body = yamlencode({
    apiVersion = "cert-manager.io/v1"
    kind       = "ClusterIssuer"
    metadata = {
      name = "ca-issuer"
    }
    spec = {
      ca = {
        secretName = "root-secret"
      }
    }
  })

  depends_on = [kubectl_manifest.ca_certificate]
}

resource "kubectl_manifest" "wildcard_cert" {
  yaml_body = yamlencode({
    apiVersion = "cert-manager.io/v1"
    kind = "Certificate"
    metadata = {
      name = "wildcard-tls"
      namespace = kubernetes_namespace.kong_system.metadata[0].name
    }
    spec = {
      secretName = "wildcard-tls-secret"
      issuerRef = {
        name =  "ca-issuer"
        kind = "ClusterIssuer"
        group = "cert-manager.io"
      }
      dnsNames = [
        "*.local"
      ]
    }
  })

  depends_on = [
    kubectl_manifest.ca_issuer,
    kubernetes_namespace.kong_system,
  ]
}

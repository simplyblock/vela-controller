resource "kubectl_manifest" "gateway_api_crds" {
  yaml_body = data.http.gateway_api_manifest.response_body
}

data "http" "gateway_api_manifest" {
  url = "https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.3.0/standard-install.yaml"
}

resource "kubernetes_namespace" "kong_system" {
  metadata {
    name = "kong-system"
  }
}

resource "helm_release" "kong_operator" {
  name       = "kong-operator"
  repository = "https://charts.konghq.com"
  chart      = "gateway-operator"
  namespace  = kubernetes_namespace.kong_system.metadata[0].name

  set {
    name  = "image.tag"
    value = "1.6.0"
  }

  set {
    name  = "global.webhooks.options.certManager.enabled"
    value = "true"
  }

  depends_on = [
    kubectl_manifest.gateway_api_crds,
    kubernetes_namespace.kong_system
  ]
}

resource "kubectl_manifest" "kong_gateway_config" {
  yaml_body = yamlencode({
    apiVersion = "gateway-operator.konghq.com/v1beta1"
    kind       = "GatewayConfiguration"
    metadata = {
      name      = "kong-gw-config"
      namespace = kubernetes_namespace.kong_system.metadata[0].name
    }
    spec = {
      dataPlaneOptions = {
        network = {
          services = {
            ingress = {
              type = "LoadBalancer"
            }
          }
        }
      }
      controlPlaneOptions = {
        deployment = {
          podTemplateSpec = {
            spec = {
              containers = [
                {
                  name  = "controller"
                  image = "kong/kubernetes-ingress-controller:3.5"
                }
              ]
            }
          }
        }
      }
    }
  })

  depends_on = [helm_release.kong_operator]
}

resource "kubectl_manifest" "kong_gateway_class" {
  yaml_body = yamlencode({
    apiVersion = "gateway.networking.k8s.io/v1"
    kind       = "GatewayClass"
    metadata = {
      name = "kong-class"
    }
    spec = {
      controllerName = "konghq.com/gateway-operator"
      parametersRef = {
        group     = "gateway-operator.konghq.com"
        kind      = "GatewayConfiguration"
        name      = "kong-gw-config"
        namespace = kubernetes_namespace.kong_system.metadata[0].name
      }
    }
  })

  depends_on = [kubectl_manifest.kong_gateway_config]
}

resource "kubectl_manifest" "kong_public_gateway" {
  yaml_body = yamlencode({
    apiVersion = "gateway.networking.k8s.io/v1"
    kind       = "Gateway"
    metadata = {
      name      = "public-gateway"
      namespace = kubernetes_namespace.kong_system.metadata[0].name
    }
    spec = {
      gatewayClassName = "kong-class"
      listeners = [
        {
          name     = "http"
          protocol = "HTTP"
          port     = 80
          allowedRoutes = {
            namespaces = {
              from = "All"
            }
          }
        },
        {
          name     = "https"
          protocol = "HTTPS"
          port     = 443
          allowedRoutes = {
            namespaces = {
              from = "All"
            }
          }
          tls = {
            mode = "Terminate"
            certificateRefs = [
              {
                name = "wildcard-tls-secret"
              }
            ]
          }
        }
      ]
    }
  })

  depends_on = [
    kubectl_manifest.kong_gateway_class,
    kubectl_manifest.wildcard_cert,
  ]
}

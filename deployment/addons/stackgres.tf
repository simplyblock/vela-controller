resource "helm_release" "stackgres_operator" {
  name       = "stackgres-operator"
  repository = "https://stackgres.io/downloads/stackgres-k8s/stackgres/helm/"
  chart      = "stackgres-operator"
  namespace  = "stackgres"

  create_namespace = true

  wait          = true
  wait_for_jobs = true
  timeout       = 600
}

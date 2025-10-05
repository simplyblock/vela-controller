provider "kubernetes" {
  config_path = "../cluster/kubeconfig.yaml"
}

provider "helm" {
  kubernetes {
    config_path = "../cluster/kubeconfig.yaml"
  }
}

provider "kubectl" {
  config_path = "../cluster/kubeconfig.yaml"
}

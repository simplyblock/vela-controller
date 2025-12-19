# GitHub Actions kubeconfig generation

Vela controller deployment workflow in `.github/workflows/deploy.yml` expects a kubeconfig stored in the `KUBECONFIG` GitHub secret.  
This document guides how the KUBECONFIG was generated

## 1. Create the service account and permissions

```sh
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: github-actions
  namespace: vela
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: github-actions-deployer
rules:
- apiGroups: [""]
  resources: [ "namespaces" ]
  verbs: [ "get", "list", "watch", "create" ]
- apiGroups: [""]
  resources:
    - serviceaccounts
    - services
    - secrets
    - configmaps
    - persistentvolumeclaims
    - pods
    - events
  verbs: [ "get", "list", "watch", "create", "update", "patch", "delete" ]
- apiGroups: [ "apps" ]
  resources:
    - deployments
    - daemonsets
    - replicasets
  verbs: [ "get", "list", "watch", "create", "update", "patch", "delete" ]
- apiGroups: [ "gateway.networking.k8s.io" ]
  resources: [ "httproutes" ]
  verbs: [ "get", "list", "watch", "create", "update", "patch", "delete" ]
- apiGroups: [ "cert-manager.io" ]
  resources: [ "certificates" ]
  verbs: [ "get", "list", "watch", "create", "update", "patch", "delete" ]
- apiGroups: [ "stackgres.io" ]
  resources: [ "sgclusters", "sginstanceprofiles" ]
  verbs: [ "get", "list", "watch", "create", "update", "patch", "delete" ]
- apiGroups: [ "rbac.authorization.k8s.io" ]
  resources:
    - clusterroles
    - clusterrolebindings
  verbs: [ "get", "list", "watch", "create", "update", "patch", "delete" ]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: github-actions-deployer
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: github-actions-deployer
subjects:
- kind: ServiceAccount
  name: github-actions
  namespace: vela
EOF
```

## 2. Create the token

```sh
TOKEN=$(kubectl -n vela create token github-actions --duration=87660h) # 10 years
echo "$TOKEN"
```

## 3. Gather cluster connection details

```sh
API_SERVER=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')
CA_CERT=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
```

## 4. Assemble the kubeconfig

```sh
cat <<EOF > kubeconfig-github-actions.yaml
apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: ${CA_CERT}
    server: ${API_SERVER}
  name: vela
contexts:
- context:
    cluster: vela
    namespace: vela
    user: github-actions
  name: github-actions@vela
current-context: github-actions@vela
users:
- name: github-actions
  user:
    token: ${TOKEN}
EOF
```

Encode the kubeconfig before saving it as the GitHub secret:

```sh
base64 -w0 kubeconfig-github-actions.yaml
```

**Note: This is token never expires. Recreate all the resources created above if the token needs to be rotated**

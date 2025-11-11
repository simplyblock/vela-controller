# GitHub Actions kubeconfig generation

Vela controller deployment workflow in `.github/workflows/deploy.yml` expects a kubeconfig stored in the `KUBECONFIG` GitHub secret.  
This document guides how the KUBECONFIG was generated

## 1. Create the service account and permissions

```sh
kubectl -n vela apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: github-actions
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: github-actions-deployer
  namespace: vela
rules:
- apiGroups: [ "apps" ]
  resources: [ "deployments" ]
  verbs: [ "get", "list", "watch", "patch", "update" ]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: github-actions-deployer
  namespace: vela
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: github-actions-deployer
subjects:
- kind: ServiceAccount
  name: github-actions
  namespace: vela
EOF
```

## 2. Create the token Secret

```sh
kubectl -n vela apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: github-actions-token
  annotations:
    kubernetes.io/service-account.name: github-actions
type: kubernetes.io/service-account-token
EOF
```

```sh
TOKEN=$(kubectl -n vela get secret github-actions-token -o jsonpath='{.data.token}' | base64 -d)
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

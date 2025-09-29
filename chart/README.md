# Kubernetes admin configuration

Ideally everything here needs to be managed using gitops methodoligy using ArgoCD or Flux. But for documenting all the admin configuration in this file. 

## Installation vela helm charts
```
helm install vela --namespace vela --create-namespace ./
```
and from then on, all the next versions can be upgraded by running 
```
helm upgrade vela --namespace vela --create-namespace ./
```

## Kong installation

Install the kong operator: 
```
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.3.0/standard-install.yaml
helm repo add kong https://charts.konghq.com
helm repo update
helm upgrade --install kong-operator kong/gateway-operator -n kong-system \
  --create-namespace \
  --set image.tag=1.6.0 \
  --set global.webhooks.options.certManager.enabled=true
```

### kong admin configuration

```
kubectl apply -f - <<'EOF'
apiVersion: gateway-operator.konghq.com/v1beta1
kind: GatewayConfiguration
metadata:
  name: kong-gw-config
  namespace: kong-system
spec:
  dataPlaneOptions:
    network:
      services:
        ingress:
          type: LoadBalancer
  controlPlaneOptions:
    deployment:
      podTemplateSpec:
        spec:
          containers:
            - name: controller
              image: kong/kubernetes-ingress-controller:3.5
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
  name: kong-class
spec:
  controllerName: konghq.com/gateway-operator
  parametersRef:
    group: gateway-operator.konghq.com
    kind: GatewayConfiguration
    name: kong-gw-config
    namespace: kong-system
EOF
```

### Setting up TLS Certificates

`public-gateway.yaml` expects `vela-run-staging-wildcard-cert` to be present in `kong-system`. So creating one. And this certificate needs to renewed manually. In production we should we cert-manager operator. 

```
kubectl create secret tls vela-run-staging-wildcard-cert \
  --cert=letsencrypt/live/staging.vela.run/fullchain.pem \
  --key=letsencrypt/live/staging.vela.run/privkey.pem \
  -n kong-system
```


### Debugging 

Once everything is installed, we should finally see this.

```
kubectl -n kong-system get svc | grep ingress-public-gateway
dataplane-ingress-public-gateway-tkzr7-vt2n7      LoadBalancer   10.103.56.9     10.10.11.10   443:31366/TCP   23m
```

If there are any issues, running these commands could give a better idea on how to whats happening
```
kubectl -n kong-system get pods
kubectl get gatewayconfigurations.gateway-operator.konghq.com -n kong-system
kubectl get gatewayclass kong-class -o yaml
kubectl get gateways -A
kubectl describe gateway public-gateway -n kong-system
kubectl get svc -n kong-system
kubectl logs -n kong-system deploy/kong-operator-gateway-operator-controller-manager
```

## Metal-LB Installation

Since we use Talos, external loadbalancer like Talos needs to be installed.

```
kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.12/config/manifests/metallb-native.yaml
```

and then apply the IP Address Pool
```
kubectl apply -f - <<'EOF'
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: default-address-pool
  namespace: metallb-system
spec:
  addresses:
  - 10.10.11.10-10.10.11.10   # exact single IP
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: advert
  namespace: metallb-system
spec: {}
EOF
```

### Metrics API server installation

kubectl top requires metrics API to be running. To install metrics API the following commands can be used

```
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
```
all the resources here are applied to `kube-system` 

check memory utilization of all pods across namespaces
```
kubectl top pod -A --sort-by=memory
```

### Kubevirt
For Kubevirt installation refer (here)[./docs/kubevirt.md]

### Cert Manager installation

Install the cert manager: 
```
helm repo add jetstack https://charts.jetstack.io
helm repo update

helm upgrade --install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.11.0 \
  --set installCRDs=true
```

### Prometheus installation

Install the prometheus: 
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm upgrade --install prometheus prometheus-community/prometheus \
  --namespace monitoring \
  --create-namespace \
  --version 25.18.0 \
  --set prometheus.server.fullnameOverride=vela-prometheus \
  --set prometheus.server.replicaCount=1 \
  --set prometheus.server.statefulSet.enabled=true \
  --set prometheus.server.persistentVolume.enabled=true \
  --set prometheus.server.persistentVolume.size=5Gi
```

### StackGres
TODO

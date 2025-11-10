
# install kong operator

```sh
helm upgrade --install kong-operator kong/gateway-operator -n kong-system \
   --create-namespace \
   --set image.tag=1.6.0 \
   --set global.webhooks.options.certManager.enabled=true
```

# create TLS certificate
```sh
 kubectl create secret tls vela-staging-cert \
   --cert=letsencrypt/live/staging.vela.run/fullchain.pem \
   --key=letsencrypt/live/staging.vela.run/privkey.pem \
   -n kong-system
```

### Create GatewayConfiguration 
```sh
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
          type: NodePort
    deployment:
      podTemplateSpec:
        spec:
          containers:
            - name: proxy
              image: kong/kong-gateway:3.9
              env:
                - name: KONG_NGINX_PROXY_PROXY_BUFFER_SIZE
                  value: "128k"
                - name: KONG_NGINX_PROXY_PROXY_BUFFERS
                  value: "4 256k"
                - name: KONG_NGINX_PROXY_PROXY_BUSY_BUFFERS_SIZE
                  value: "256k"
EOF
```

### Create GatewayClass

```sh
kubectl apply -f - <<'EOF'
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata:
    name: kong-class
    namespace: kong-system
spec:
    controllerName: konghq.com/gateway-operator
    parametersRef:
        group: gateway-operator.konghq.com
        kind: GatewayConfiguration
        name: kong-gw-config
        namespace: kong-system
EOF
```

### create Gateway
```sh
kubectl apply -f - <<'EOF'
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
    name: vela-public-gateway
    namespace: kong-system
spec:
  gatewayClassName: kong-class
  listeners:
  - allowedRoutes:
      namespaces:
        from: All
    name: http
    port: 80
    protocol: HTTP
  - allowedRoutes:
      namespaces:
        from: All
    name: https
    port: 443
    protocol: HTTPS
    tls:
      certificateRefs:
      - group: ""
        kind: Secret
        name: vela-staging-cert
      mode: Terminate
EOF
```

# KubeOVN

```sh
# label any of the one worker node with this
kubectl label node talos-pub-abo kube-ovn/role=master
```
And then install the OVN plugin
```sh
helm repo update
helm install kube-ovn kubeovn/kube-ovn --wait \
    -n kube-system \
    --version v1.14.10 \
    --set OVN_DIR=/var/lib/ovn \
    --set OPENVSWITCH_DIR=/var/lib/openvswitch \
    --set DISABLE_MODULES_MANAGEMENT=true \
    --set cni_conf.MOUNT_LOCAL_BIN_DIR=false
```

#### allow OVN to allocate IPv6 addresses to pods

Make sure that the Talos cluster support IPv6 addresses. If the `cluster.network.podSubnets` and `cluster.network.serviceSubnets` has Ipv6 addresses, then the cluster support dual Stack. 

Patch the `ovn-default` subnet to also support support IPv6.

First off, get the current ipv4 subnets by running
`kubectl get subnet ovn-default -o yaml | grep -E 'protocol|cidrBlock|gateway'` 

```
kubectl patch subnet ovn-default \
  --type merge \
  -p '{"spec":{
    "protocol":"Dual",
    "cidrBlock":"10.16.0.0/16,fd10:244::/56",
    "gateway":"10.16.0.1,fd10:244::1",
    "excludeIps":["10.16.0.1","fd10:244::1"]
  }}'
```

# StackGres

```sh
helm upgrade --install stackgres-operator \
  stackgres-operator \
  --repo https://stackgres.io/downloads/stackgres-k8s/stackgres/helm/ \
  --namespace stackgres \
  --create-namespace \
  --wait \
  --timeout 600s
```

### Cert Manager
```
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --version v1.13.0 --set installCRDs=true
```

after installation, create create self-signed certificate issuer `ca-issuer`. For production we should use LetsEncrypt
```sh
kubectl apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-bootstrap
spec:
  selfSigned: {}

---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ca-cert
  namespace: cert-manager
spec:
  isCA: true
  commonName: vela-ca
  secretName: ca-key-pair
  issuerRef:
    name: selfsigned-bootstrap
    kind: ClusterIssuer

---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ca-issuer
spec:
  ca:
    secretName: ca-key-pair
EOF
```

### Monitoring
Disabled node exporter so ask to not to conflict with the onces already exists in cluster
Ideally simplyblock should able to hook into the existing monitoring solution. But implementing that feedback was never
prioritised: https://github.com/simplyblock/sbcli/pull/408#issuecomment-2991065338

```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update

helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set grafana.enabled=false \
  --set alertmanager.enabled=false \
  --set nodeExporter.enabled=false \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.storageClassName=openebs-local-hostpath \
  --set prometheus.prometheusSpec.storageSpec.volumeClaimTemplate.spec.resources.requests.storage=5Gi
```

### stackgres

```
hehelm upgrade --install stackgres-operator \
  stackgres-operator \
  --repo https://stackgres.io/downloads/stackgres-k8s/stackgres/helm/ \
  --namespace stackgres \
  --create-namespace \
  --wait \
  --timeout 600s \
  --version 1.17.4
```

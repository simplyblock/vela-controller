### installation

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

`public-gateway.yaml` expects `wildcard-cert` to be present in `kong-system`. So creating one. And this certificate needs to renewed manually. In production we should we cert-manager operator. 

```
kubectl create secret tls vela-run-staging-wildcard-cert \
  --cert=letsencrypt/live/staging.vela.run/fullchain.pem \
  --key=letsencrypt/live/staging.vela.run/privkey.pem \
  -n kong-system
```


Due the conflits for port 443 and 80, I've exposed the public gateway as a `NodePort`. But in production this will be in 
Loadbalancer. 

```
kubectl -n kong-system get svc | grep ingress-public-gateway
NAME                                              TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)         AGE
dataplane-ingress-public-gateway-qgwn4-h4m4j      NodePort    10.43.168.43   <none>        443:31871/TCP   21m
```

If the node IP of your k8s cluster is `192.168.10.146`, this is the public IP of Kong. 

### Debugging 

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

### Metal-LB

For the server to have external traffic

```
kukubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.12/config/manifests/metallb-native.yaml
```

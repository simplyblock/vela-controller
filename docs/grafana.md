For PG Exporter to work, prometheus operator needs to be instaled. 
```
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

install the helm chart
```
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace
```

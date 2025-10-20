### using for local development

During local development, Logflare, Grafana, Vector, Studio, and certificate provisioning can be disabled if not needed.
```
helm upgrade --install vela --namespace vela --create-namespace ./chart/ \
    --set vector.enabled=false \
    --set studio.enabled=false
```

you can start using by port forwarding the services
```
kubectl -n vela port-forward svc/vela-controller-service 8000:8000
kubectl -n vela port-forward svc/vela-auth-service 8080:8080
```

Get a token
```
export TOKEN=$(curl -s http://localhost:8080/auth/realms/vela/protocol/openid-connect/token   -H "Content-Type: application/x-www-form-urlencoded"   -d "username=testuser"   -d "password=testpassword"    -d "grant_type=password"   -d "client_id=frontend" -d "client_secret=client-secret" | jq -r '.access_token')
```


### production usage

```
helm install vela --namespace vela --create-namespace ./chart/
```

# Supabase Kubernetes

This repository contains the charts to deploy a [Supabase](https://github.com/supabase/supabase) instance inside a Kubernetes cluster using Helm 3.

For any information regarding Supabase itself you can refer to the [official documentation](https://supabase.io/docs).

## What's Supabase ?

Supabase is an open source Firebase alternative. We're building the features of Firebase using enterprise-grade open source tools.

## How to use ?

You can find the documentation inside the [chart directory](./charts/supabase/README.md)

# Roadmap

- [ ] Multi-node Support

## Support

This project is supported by the community and not officially supported by Supabase. Please do not create any issues on the official Supabase repositories if you face any problems using this project, but rather open an issue on this repository.

## Contributing

You can contribute to this project by forking this repository and opening a pull request.

When you're ready to publish your chart on the `main` branch, you'll have to execute `sh build.sh` to package the charts and generate the Helm manifest.


### Kubevirt Installation

#### Install the operator
```
export VERSION=$(curl -s https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)
echo $VERSION
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-operator.yaml"
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-cr.yaml"
```

Update Kubevirt CR and enable live migration & live update
```
kubectl patch kubevirt kubevirt -n kubevirt --type=merge -p '{"spec":{"configuration":{"developerConfiguration":{"featureGates":["LiveMigration"]}}}}'
kubectl --namespace kubevirt patch kv kubevirt -p='[{"op": "add", "path": "/spec/configuration/vmRolloutStrategy", "value": "LiveUpdate"}]' --type='json'
kubectl --namespace kubevirt patch kv kubevirt -p='[{"op": "add", "path": "/spec/workloadUpdateStrategy/workloadUpdateMethods", "value": ["LiveMigrate"]}]' --type='json'
```

Verify if all the components are installed

```
kubectl get kubevirt.kubevirt.io/kubevirt -n kubevirt -o=jsonpath="{.status.phase}"
```

## License

[Apache 2.0 License.](./LICENSE)

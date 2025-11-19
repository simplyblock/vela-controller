# Kubevirt Installation

## Install the operator
```
export VERSION=$(curl -s https://storage.googleapis.com/kubevirt-prow/release/kubevirt/kubevirt/stable.txt)
echo $VERSION
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-operator.yaml"
kubectl create -f "https://github.com/kubevirt/kubevirt/releases/download/${VERSION}/kubevirt-cr.yaml"
```

Update Kubevirt CR and enable live migration & live update
```
kubectl patch kubevirt kubevirt -n kubevirt --type=merge -p '{"spec":{"configuration":{"developerConfiguration":{"featureGates":["LiveMigration", "ExpandDisks"]}}}}'
kubectl --namespace kubevirt patch kv kubevirt -p='[{"op": "add", "path": "/spec/configuration/vmRolloutStrategy", "value": "LiveUpdate"}]' --type='json'
kubectl --namespace kubevirt patch kv kubevirt -p='[{"op": "add", "path": "/spec/workloadUpdateStrategy/workloadUpdateMethods", "value": ["LiveMigrate"]}]' --type='json'
```

Verify if all the components are installed

```
kubectl get kubevirt.kubevirt.io/kubevirt -n kubevirt -o=jsonpath="{.status.phase}"
```

### Install krew plugin

[Krew](https://krew.sigs.k8s.io) is the plugin manager for kubectl command-line tool. If krew is not installed, please follow [this](https://krew.sigs.k8s.io/docs/user-guide/setup/install/) guide to install it.

After Krew is installed,  the virt plugin can be installed:
```
kubectl krew install virt
```

### Debugging tips

To list of existing VMs in a namespace

```
$ kubectl -n vela-deployment-4 get virtualmachineinstances 
NAME                                     AGE    PHASE     IP           NODENAME                        READY
vela-vela-deployment-4-vela-db   3h9m   Running   10.42.1.50   vm07.simplyblock5.localdomain   True
```

To connect to the VM console. Note: At any point of time, only one instance of console can be kept runing
```
kubectl virt -n vela-deployment-4 console vela-vela-deployment-4-vela-db
```



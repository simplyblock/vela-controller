## Neon Autoscaler

### Setup

Autoscaler expects cert-manager to be present in the cluster. Setup instructions are available [here](./manual-deployment.md#cert-manager).


```
kubectl apply -f https://raw.githubusercontent.com/k8snetworkplumbingwg/multus-cni/master/deployments/multus-daemonset-thick.yml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/whereabouts.yaml
```


Install the Neon autoscaler controllers in the `neonvm-system` namespace.

```sh
kubectl create ns neonvm-system
kubectl label namespace neonvm-system  \
  pod-security.kubernetes.io/enforce=privileged --overwrite \
  pod-security.kubernetes.io/audit=privileged --overwrite \
  pod-security.kubernetes.io/warn=privileged --overwrite
```


```sh
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/neonvm.yaml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/neonvm-vxlan-controller.yaml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/neonvm-controller.yaml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/autoscale-scheduler.yaml
kubectl apply -f https://github.com/neondatabase/autoscaling/releases/latest/download/autoscaler-agent.yaml
```

Since the new CRDs manages the PVCs, we need to update `neonvm-manager-role` cluster role permissions

```
kubectl patch clusterrole neonvm-manager-role \
  --type='json' \
  -p='[{"op":"add","path":"/rules/0","value":{"apiGroups":[""],"resources":["persistentvolumeclaims"],"verbs":["get","list","watch","create","update","patch","delete"]}}]'
kubectl delete pod -n neonvm-system -l control-plane=controller 
```

Edit the scaling unit to match the steps the vela-controller assumes. Modify `data.config.json.scaling.computeUnit`.
The relevant values are defined in `VCPU_MILLIS_STEP` and `MEMORY_STEP`.
```
kubectl -n kube-system edit configmap autoscaler-agent-config
```


Apply the required NeonVM CRDs:

```
kubectl apply -f https://raw.githubusercontent.com/simplyblock/autoscaling/refs/heads/main/neonvm/config/crd/bases/vm.neon.tech_virtualmachines.yaml
kubectl apply -f https://raw.githubusercontent.com/simplyblock/autoscaling/refs/heads/main/neonvm/config/crd/bases/vm.neon.tech_virtualmachinemigrations.yaml
```

Demo environment uses the images that are manually deployed. 

```
# deploy scheduler
kubectl -n kube-system \
  set image deployment/autoscale-scheduler \
  autoscale-scheduler=docker.io/simplyblock/autoscaling:autoscale-scheduler-main-7e22f4a
kubectl -n kube-system rollout status deployment/autoscale-scheduler 

# deploy autoscaler agent
kubectl -n kube-system \
  set image daemonset/autoscaler-agent \
  autoscaler-agent=docker.io/simplyblock/autoscaling:autoscaler-agent-main-7e22f4a
kubectl -n kube-system rollout status daemonset/autoscaler-agent

# deploy vxlan controller
kubectl -n neonvm-system \
  set image daemonset/neonvm-vxlan-controller \
  vxlan-controller=docker.io/simplyblock/autoscaling:neonvm-vxlan-controller-main-7e22f4a
kubectl -n neonvm-system rollout status daemonset/neonvm-vxlan-controller

kubectl -n neonvm-system \
  set image deployment/neonvm-controller \
  manager=docker.io/simplyblock/autoscaling:neonvm-controller-main-7e22f4a
kubectl -n neonvm-system rollout status deployment/neonvm-controller

kubectl -n neonvm-system \
  set image daemonset/neonvm-runner-image-loader \
  neonvm-runner-loader=docker.io/simplyblock/autoscaling:neonvm-runner-main-7e22f4a
kubectl -n neonvm-system rollout status daemonset/neonvm-runner-image-loader

kubectl -n neonvm-system set env deployment/neonvm-controller \
  VM_RUNNER_IMAGE=docker.io/simplyblock/autoscaling:neonvm-runner-main-7e22f4a
kubectl -n neonvm-system rollout status deployment/neonvm-controller
```

### Usage

Create a sample VM:
```sh
cat <<EOF | kubectl apply -f -
apiVersion: vm.neon.tech/v1
kind: VirtualMachine
metadata:
  name: vm-manohar-dev
spec:
  powerState: Running
  extraNetwork:
    enable: true
  guest:
    rootDisk:
      image: docker.io/manoharbrm/pg16-test:dev9 # TODO: update image
    cpus: { min: 1, use: 1, limit: 64, max: 64 }
    memorySlots: { min: 4, use: 4, limit: 16, max: 16 }
    memorySlotSize: 128Mi
  disks:
    - name: data
      mountPath: /var/lib/postgresql/data
      blockDevice:
        persistentVolumeClaim:
          storageClassName: simplyblock-csi-sc
          accessModes:
            - ReadWriteMany
          resources:
            requests:
              storage: 20Gi
EOF
```

Get the IP address, connect to the VM, and run a pgbench test:

```bash
kubectl run pgbench --rm -it \
  --image=postgres:17 \
  --overrides='
{
  "apiVersion": "v1",
  "metadata": {
    "annotations": {
      "k8s.v1.cni.cncf.io/networks": "neonvm-system/neonvm-overlay-for-pods"
    }
  }
}
' \
  --env PGPASSWORD=manohar_pd -- \
  bash -c "
    pgbench -h 10.100.128.0 -p 5432 -U postgres -i -s 10 postgres &&
    pgbench -h 10.100.128.0 -p 5432 -U postgres -c 4 -j 2 -T 60 -P postgres
  "
```
